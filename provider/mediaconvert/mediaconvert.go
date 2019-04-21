// Package mediaconvert provides a implementation of the provider that
// uses AWS MediaConvert for transcoding media files.
//
// It doesn't expose any public type. In order to use the provider, one must
// import this package and then grab the factory from the provider package:
//
//     import (
//         "github.com/NYTimes/video-transcoding-api/provider"
//         "github.com/NYTimes/video-transcoding-api/provider/mediaconvert"
//     )
//
//     func UseProvider() {
//         factory, err := provider.GetProviderFactory(mediaconvert.Name)
//         // handle err and use factory to get an instance of the provider.
//     }
package mediaconvert // import "github.com/NYTimes/video-transcoding-api/provider/mediaconvert"

import (
	"errors"
	"fmt"

	// "fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	// "time"

	"github.com/NYTimes/video-transcoding-api/config"
	"github.com/NYTimes/video-transcoding-api/db"
	"github.com/NYTimes/video-transcoding-api/provider"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elastictranscoder"
	"github.com/aws/aws-sdk-go/service/elastictranscoder/elastictranscoderiface"
	"github.com/aws/aws-sdk-go/service/mediaconvert"
)

const (
	// Name is the name used for registering the Elastic Transcoder
	// provider in the registry of providers.
	Name = "mediaconvert"

	defaultAWSRegion = "us-east-1"
	hlsPlayList      = "HLSv3"
)

var (
	errAWSInvalidConfig = errors.New("invalid Elastic Transcoder config. Please define the configuration entries in the config file or environment variables")
	s3Pattern           = regexp.MustCompile(`^s3://`)
)

func init() {
	provider.Register(Name, mediaconvertFactory)
}

type awsProvider struct {
	c      mediaconvert.MediaConvert
	ce     elastictranscoderiface.ElasticTranscoderAPI
	config *config.MediaConvert
}

func (p *awsProvider) Transcode(job *db.Job) (*provider.JobStatus, error) {
	//var adaptiveStreamingOutputs []db.TranscodeOutput

	source := job.SourceMedia
	params := mediaconvert.CreateJobInput{
		Queue: aws.String(p.config.Queue),
		Role: aws.String(p.config.Role),
		Settings: &mediaconvert.JobSettings{
			Inputs: []*mediaconvert.Input{
				&mediaconvert.Input{
					FileInput: aws.String(source),
					AudioSelectors: map[string]*mediaconvert.AudioSelector{
						"Audio Selector 1": &mediaconvert.AudioSelector {
							DefaultSelection: aws.String("DEFAULT"),
							Offset: aws.Int64(0),
							ProgramSelection: aws.Int64(1),
						},
					},
					VideoSelector: &mediaconvert.VideoSelector{
						ColorSpace: aws.String("FOLLOW"),
					},
				},
			},
		},
	}
	params.Settings.OutputGroups = make([]*mediaconvert.OutputGroup, len(job.Outputs))
	for i, output := range job.Outputs {
		presetID, ok := output.Preset.ProviderMapping[Name]
		if !ok {
			return nil, provider.ErrPresetMapNotFound
		}

		presetOutput, err := p._GetPreset(presetID)
		if err != nil {
			return nil, err
		}
		if presetOutput.Preset == nil || presetOutput.Preset.Type == nil {
			return nil, fmt.Errorf("misconfigured preset: %s", presetID)
		}

		var isAdaptiveStreamingPreset bool
		if *presetOutput.Preset.Settings.ContainerSettings.Container == "M3U8" {
			isAdaptiveStreamingPreset = true
			// adaptiveStreamingOutputs = append(adaptiveStreamingOutputs, output)
		}

		params.Settings.OutputGroups[i] = &mediaconvert.OutputGroup{
			Outputs: []*mediaconvert.Output{&mediaconvert.Output{
				Preset: aws.String(presetID),
				NameModifier: aws.String("test3"),
			}},
			OutputGroupSettings: &mediaconvert.OutputGroupSettings{},
		}

		// hardcode
		params.Settings.OutputGroups[i].OutputGroupSettings.SetType("HLS_GROUP_SETTINGS")

		params.Settings.OutputGroups[i].OutputGroupSettings.HlsGroupSettings = &mediaconvert.HlsGroupSettings{
			Destination: p.outputKey(job, output.FileName, isAdaptiveStreamingPreset, p.config.Destination),
			DirectoryStructure: aws.String("SINGLE_DIRECTORY"),
			// SegmentControl: aws.String("SEGMENTED_FILES"),
			MinSegmentLength: aws.Int64(0),
		}

		if isAdaptiveStreamingPreset {
			segmentLength := int64(job.StreamingParams.SegmentDuration)
			params.Settings.OutputGroups[i].OutputGroupSettings.HlsGroupSettings.SegmentLength = &segmentLength
		}
	}

	resp, err := p.c.CreateJob(&params)
	if err != nil {
		return nil, err
	}
	return &provider.JobStatus{
		ProviderName:  Name,
		ProviderJobID: aws.StringValue(resp.Job.Id),
		Status:        provider.StatusQueued,
	}, nil
}

func (p *awsProvider) normalizeSource(source string) string {
	if s3Pattern.MatchString(source) {
		source = strings.Replace(source, "s3://", "", 1)
		parts := strings.SplitN(source, "/", 2)
		return parts[len(parts)-1]
	}
	return source
}

func (p *awsProvider) outputKey(job *db.Job, fileName string, adaptive bool, destination string) *string {
	if adaptive {
		fileName = strings.TrimRight(fileName, filepath.Ext(fileName))
	}
	baseLocation := strings.TrimRight(destination, "/")

	return aws.String(baseLocation + "/" + job.ID + "/" + fileName)
}

func (p *awsProvider) createVideoPreset(preset db.Preset) *mediaconvert.VideoDescription {
	videoPreset := mediaconvert.VideoDescription {
		CodecSettings: &mediaconvert.VideoCodecSettings{
			H264Settings: &mediaconvert.H264Settings{},
		},
		ScalingBehavior: aws.String("DEFAULT"),
		TimecodeInsertion: aws.String("DISABLED"),
		AntiAlias: aws.String("ENABLED"),
		// Sharpness: int64(50), // TODO: fix hardcode
		AfdSignaling: aws.String("NONE"),
		DropFrameTimecode: aws.String("ENABLED"),
		RespondToAfd: aws.String("NONE"),
		ColorMetadata: aws.String("INSERT"),
	}

	switch preset.Video.Codec {
	case "h264":
		videoPreset.CodecSettings.Codec = aws.String("H_264")

		normalizedVideoBitRate, _ := strconv.Atoi(preset.Video.Bitrate)
		videoBitrate := int64(normalizedVideoBitRate)
		videoPreset.CodecSettings.H264Settings.Bitrate = &videoBitrate
		normalizedGopSize, _ := strconv.Atoi(preset.Video.GopSize)
		gopSize := float64(normalizedGopSize)
		videoPreset.CodecSettings.H264Settings.GopSize = &gopSize
		videoPreset.CodecSettings.H264Settings.RateControlMode = &preset.RateControl

		videoPreset.CodecSettings.H264Settings.SetCodecProfile(strings.ToUpper(preset.Video.Profile))
		videoPreset.CodecSettings.H264Settings.SetCodecLevel("AUTO")
	}

	return &videoPreset
}

func (p *awsProvider) createAudioPreset(preset db.Preset) *mediaconvert.AudioDescription {
	audioPreset := mediaconvert.AudioDescription{
		CodecSettings: &mediaconvert.AudioCodecSettings{
			AacSettings: &mediaconvert.AacSettings{},
		},
	}

	normalizedAudioBitRate, _ := strconv.Atoi(preset.Audio.Bitrate)
	audioBitrate := int64(normalizedAudioBitRate)

	switch preset.Audio.Codec {
	case "aac":
		audioPreset.CodecSettings.Codec = aws.String("AAC")
		audioPreset.CodecSettings.AacSettings.SetCodecProfile("LC") // TODO: remove hardcode
		audioPreset.CodecSettings.AacSettings.CodingMode = aws.String("CODING_MODE_2_0")
		audioPreset.CodecSettings.AacSettings.SetSampleRate(48000)

		audioPreset.CodecSettings.AacSettings.SetRateControlMode("CBR")
		audioPreset.CodecSettings.AacSettings.Bitrate = &audioBitrate
	}

	return &audioPreset
}

func (p *awsProvider) CreatePreset(preset db.Preset) (string, error) {
	presetInput := mediaconvert.CreatePresetInput{
		Name:        &preset.Name,
		Description: &preset.Description,
		Settings: &mediaconvert.PresetSettings{
			ContainerSettings: &mediaconvert.ContainerSettings{},
		},
	}
	if preset.Container == "m3u8" {
		presetInput.Settings.ContainerSettings.Container = aws.String("M3U8")
	} else {
		presetInput.Settings.ContainerSettings.Container = &preset.Container
	}

	presetInput.Settings.VideoDescription = p.createVideoPreset(preset)
	presetInput.Settings.AudioDescriptions = []*mediaconvert.AudioDescription{p.createAudioPreset(preset)}
	presetOutput, err := p.c.CreatePreset(&presetInput)
	if err != nil {
		return "", err
	}
	return *presetOutput.Preset.Name, nil
}

func (p *awsProvider) _GetPreset(presetID string) (*mediaconvert.GetPresetOutput, error) {
	getPresetInput := &mediaconvert.GetPresetInput{
		Name: aws.String(presetID),
	}
	getPresetOutput, err := p.c.GetPreset(getPresetInput)
	if err != nil {
		return nil, err
	}
	return getPresetOutput, err
}

func (p *awsProvider) GetPreset(presetID string) (interface{}, error) {
	getPresetOutput, err := p._GetPreset(presetID)
	if err != nil {
		return nil, err
	}
	return getPresetOutput, err
}

func (p *awsProvider) DeletePreset(presetID string) error {
	presetInput := mediaconvert.DeletePresetInput{
		Name: &presetID,
	}
	_, err := p.c.DeletePreset(&presetInput)
	return err
}

func (p *awsProvider) JobStatus(job *db.Job) (*provider.JobStatus, error) {
	/*
	id := job.ProviderJobID
	resp, err := p.c.ReadJob(&elastictranscoder.ReadJobInput{Id: aws.String(id)})
	if err != nil {
		return nil, err
	}
	totalJobs := len(resp.Job.Outputs)
	completedJobs := float64(0)
	outputs := make(map[string]interface{}, totalJobs)
	for _, output := range resp.Job.Outputs {
		outputStatus := p.statusMap(aws.StringValue(output.Status))
		switch outputStatus {
		case provider.StatusFinished, provider.StatusCanceled, provider.StatusFailed:
			completedJobs++
		}
		outputs[aws.StringValue(output.Key)] = aws.StringValue(output.StatusDetail)
	}
	outputDestination, err := p.getOutputDestination(job, resp.Job)
	if err != nil {
		outputDestination = err.Error()
	}
	outputFiles, err := p.getOutputFiles(resp.Job)
	if err != nil {
		return nil, err
	}
	var sourceInfo provider.SourceInfo
	if resp.Job.Input.DetectedProperties != nil {
		sourceInfo = provider.SourceInfo{
			Duration: time.Duration(aws.Int64Value(resp.Job.Input.DetectedProperties.DurationMillis)) * time.Millisecond,
			Height:   aws.Int64Value(resp.Job.Input.DetectedProperties.Height),
			Width:    aws.Int64Value(resp.Job.Input.DetectedProperties.Width),
		}
	}
	statusMessage := ""
	if len(resp.Job.Outputs) > 0 {
		statusMessage = aws.StringValue(resp.Job.Outputs[0].StatusDetail)
		if strings.Contains(statusMessage, ":") {
			errorMessage := strings.SplitN(statusMessage, ":", 2)[1]
			statusMessage = strings.TrimSpace(errorMessage)
		}
	}
	return &provider.JobStatus{
		ProviderJobID:  aws.StringValue(resp.Job.Id),
		Status:         p.statusMap(aws.StringValue(resp.Job.Status)),
		StatusMessage:  statusMessage,
		Progress:       completedJobs / float64(totalJobs) * 100,
		ProviderStatus: map[string]interface{}{"outputs": outputs},
		SourceInfo:     sourceInfo,
		Output: provider.JobOutput{
			Destination: outputDestination,
			Files:       outputFiles,
		},
	}, nil
	*/

	return &provider.JobStatus{
		ProviderJobID:  "0",
		Status:         "0",
		StatusMessage:  "",
	}, nil
}

func (p *awsProvider) getOutputDestination(job *db.Job, awsJob *elastictranscoder.Job) (string, error) {
	/*
	readPipelineOutput, err := p.c.ReadPipeline(&elastictranscoder.ReadPipelineInput{
		Id: awsJob.PipelineId,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("s3://%s/%s",
		aws.StringValue(readPipelineOutput.Pipeline.OutputBucket),
		job.ID,
	), nil

	 */
	return "", nil
}

func (p *awsProvider) getOutputFiles(job *elastictranscoder.Job) ([]provider.OutputFile, error) {
	/*
	pipeline, err := p.c.ReadPipeline(&elastictranscoder.ReadPipelineInput{
		Id: job.PipelineId,
	})
	if err != nil {
		return nil, err
	}
	*/
	files := make([]provider.OutputFile, 0, len(job.Outputs)+len(job.Playlists))
	/*
	for _, output := range job.Outputs {
		preset, err := p.c.ReadPreset(&elastictranscoder.ReadPresetInput{
			Id: output.PresetId,
		})
		if err != nil {
			return nil, err
		}
		filePath := fmt.Sprintf("s3://%s/%s%s",
			aws.StringValue(pipeline.Pipeline.OutputBucket),
			aws.StringValue(job.OutputKeyPrefix),
			aws.StringValue(output.Key),
		)
		container := aws.StringValue(preset.Preset.Container)
		if container == "ts" {
			continue
		}
		file := provider.OutputFile{
			Path:       filePath,
			Container:  container,
			VideoCodec: aws.StringValue(preset.Preset.Video.Codec),
			Width:      aws.Int64Value(output.Width),
			Height:     aws.Int64Value(output.Height),
		}
		files = append(files, file)
	}
	for _, playlist := range job.Playlists {
		filePath := fmt.Sprintf("s3://%s/%s%s",
			aws.StringValue(pipeline.Pipeline.OutputBucket),
			aws.StringValue(job.OutputKeyPrefix),
			aws.StringValue(playlist.Name)+".m3u8",
		)
		files = append(files, provider.OutputFile{Path: filePath, Container: "m3u8"})
	}
	*/
	return files, nil
}

func (p *awsProvider) statusMap(awsStatus string) provider.Status {
	switch awsStatus {
	case "Submitted":
		return provider.StatusQueued
	case "Progressing":
		return provider.StatusStarted
	case "Complete":
		return provider.StatusFinished
	case "Canceled":
		return provider.StatusCanceled
	default:
		return provider.StatusFailed
	}
}

func (p *awsProvider) CancelJob(id string) error {
	_, err := p.c.CancelJob(&mediaconvert.CancelJobInput{Id: aws.String(id)})
	return err
}


func (p *awsProvider) Healthcheck() error {

	return nil
	/*
	_, err := p.c.ReadPipeline(&elastictranscoder.ReadPipelineInput{
		Id: aws.String(p.config.PipelineID),
	})
	return err
	*/
}

func (p *awsProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		InputFormats:  []string{"h264"},
		OutputFormats: []string{"mp4", "hls"},
		Destinations:  []string{"s3"},
	}
}

func mediaconvertFactory(cfg *config.Config) (provider.TranscodingProvider, error) {
	if cfg.MediaConvert.AccessKeyID == "" || cfg.MediaConvert.SecretAccessKey == "" || cfg.MediaConvert.Endpoint == "" || cfg.MediaConvert.Queue == "" {
		return nil, errAWSInvalidConfig
	}
	creds := credentials.NewStaticCredentials(cfg.MediaConvert.AccessKeyID, cfg.MediaConvert.SecretAccessKey, "")
	region := cfg.MediaConvert.Region
	if region == "" {
		region = defaultAWSRegion
	}
	awsSession, err := session.NewSession(aws.NewConfig().WithCredentials(creds).WithRegion(region))
	if err != nil {
		return nil, err
	}

	c := mediaconvert.New(awsSession, aws.NewConfig().WithEndpoint(cfg.MediaConvert.Endpoint))
	//c.Config.WithEndpoint(cfg.MediaConvert.Endpoint)

	return &awsProvider{
		c:      *c,
		config: cfg.MediaConvert,
	}, nil
}
