package mediaconverter

import (
	"fmt"
	"os/exec"
	"strings"
)

func StartWorkers(count int) {
	for i := 0; i < count; i++ {
		go worker()
	}
}

func worker() {
	for jobID := range ProcessingQueue {

		job, ok := Jobs.Get(jobID)
		if !ok {
			println("job not found")
			continue
		}

		job.SetStatus(JobStatusProcessing)

		err := convertMedia(job.InputPath, job.OutputPath, job.TargetFormat)

		if err != nil {
			fmt.Printf("[Error] [Job] While processing file: %v\n", err)
			job.Fail(err.Error())
			continue
		}

		job.Complete()
	}

}

func isSupportedTarget(format string) bool {
	switch format {
	case "avif", "webp", "jpgxl", "jxl", "h265", "hevc", "av1", "opus", "flac":
		return true
	default:
		return false
	}
}

func outputExtension(format string) string {
	switch format {
	case "jpgxl", "jxl":
		return ".jxl"
	case "h265", "hevc":
		return ".mp4"
	case "av1":
		return ".mkv"
	default:
		return "." + format
	}
}

func convertMedia(inputPath, outputPath, format string) error {
	args := []string{"-y", "-i", inputPath}
	switch format {
	case "avif":
		args = append(args, "-c:v", "libaom-av1", "-still-picture", "1")
	case "webp":
		args = append(args, "-c:v", "libwebp")
	case "jpgxl", "jxl":
		args = append(args, "-c:v", "libjxl")
	case "h265", "hevc":
		args = append(args, "-c:v", "libx265")
	case "av1":
		args = append(args, "-c:v", "libaom-av1")
	case "opus":
		args = append(args, "-c:a", "libopus")
	case "flac":
		args = append(args, "-c:a", "flac")
	}
	args = append(args, outputPath)

	cmd := exec.Command("ffmpeg", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

//job.SetStatus(mediaconverter.JobStatusConverting)
//
//if err := convertMedia(job.InputPath, job.OutputPath, job.TargetFormat); err != nil {
//job.Fail(err.Error())
//return
//}
//job.Complete()
