import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.bramp.ffmpeg.FFmpeg;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.FFprobe;
import net.bramp.ffmpeg.builder.FFmpegBuilder;

import java.io.File;


@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public @Data
class Worker {

    @NonNull FFmpeg ffmpeg;
    @NonNull FFprobe ffprobe;
    @NonNull
    private String workerName;
    private JobStatus currentJob;

    /**
     * This method will return a job that should be used to contact the server for another job
     *
     * @return
     */
    public static JobStatus generateJobForRequestingNewJob(String workerName) {
        JobStatus jobStatus = new JobStatus();
        jobStatus.setJob(null);
        jobStatus.setComplete(false);
        jobStatus.setWorkerSignature(workerName);
        return jobStatus;
    }

    public void executeJob(String bucketName) {
        Job job = currentJob.getJob();
        S3Repository s3Repository = new S3Repository(bucketName);

        System.out.println("downloading from s3 " + job.getOldvideofilename());
        s3Repository.downloadFile(job.getOldvideofilename(), job.getOldvideofilename());

        System.out.println("Encoding file " + job.getOldvideofilename());
        FFmpegBuilder builder = new FFmpegBuilder()
                .setInput(job.getOldvideofilename())
                .overrideOutputFiles(true)
                .addOutput(job.getNewvideofilename())
                .setFormat("mp4")
                .setConstantRateFactor(job.getCrf())
                .disableSubtitle()
                .setAudioChannels(1)
                .setAudioCodec("aac")
                .setAudioSampleRate(48000)
                .setAudioBitRate(job.getAudiobitrate())
                .setVideoCodec("libx264")
                .setVideoFrameRate(30, 1)
                .setVideoResolution(job.getWidth(), job.getHeight())
                .setStrict(FFmpegBuilder.Strict.EXPERIMENTAL)
                .done();

        FFmpegExecutor executor = new FFmpegExecutor(ffmpeg, ffprobe);
        executor.createJob(builder).run();


        System.out.println("uploading transcoded file " + job.getNewvideofilename());
        s3Repository.putFile(new File(job.getNewvideofilename()), job.getNewvideofilename());

        System.out.println("removing the work files from local machine");
        //remove files
        File raw = new File(job.getOldvideofilename());
        raw.delete();
        File transcoded = new File(job.getNewvideofilename());
        transcoded.delete();
    }
}
