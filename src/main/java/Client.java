import com.google.gson.Gson;
import net.bramp.ffmpeg.FFmpeg;
import net.bramp.ffmpeg.FFprobe;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class Client {

    private static String hostname = null;
    private static int portNumber;

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            printUsage();
            System.exit(1);
        }

        String bucketName = null;
        String workerName = null;
        String pathToFFmpeg = null;
        String pathToFFprobe = null;
        Worker worker = null;

        try {
            hostname = args[0];
            portNumber = Integer.parseInt(args[1]);
            bucketName = args[2];
            workerName = args[3];
            pathToFFmpeg = args[4];
            pathToFFprobe = args[5];
        } catch (NumberFormatException numberFormatException) {
            System.out.println("ERR - arg 2");
            System.exit(1);
        }

        FFmpeg fFmpeg = new FFmpeg(pathToFFmpeg);
        FFprobe fFprobe = new FFprobe(pathToFFprobe);
        worker = new Worker(fFmpeg, fFprobe, workerName);

        while (true) {
            System.out.println("polling server");
            JobStatus jobStatus = PollServerForJob(10000, worker);
            System.out.println(worker.getWorkerName() + " Executing");
            worker.setCurrentJob(jobStatus);
            worker.executeJob(bucketName);
            worker.getCurrentJob().setComplete(true);
            finalizeJobWithServer(worker);
            System.out.println("sending finalized job");
        }

    }


    private static JobStatus PollServerForJob(long durationBetweenPollMillis, Worker worker) {
        BufferedReader inFromServer = null;
        DataOutputStream dataOutputStream = null;
        Gson gson = new Gson();
        JobStatus outJob = Worker.generateJobForRequestingNewJob(worker.getWorkerName());
        Socket socket = null;
        JobStatus newJob = null;

        while (true) {
            try {
                //set up
                socket = new Socket(hostname, portNumber);
                inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                dataOutputStream = new DataOutputStream(socket.getOutputStream());


                System.out.println("sending tcp");
                //request new job
                dataOutputStream.writeBytes(outJob.toJson() + "\n");
                String inJson = inFromServer.readLine();

                newJob = gson.fromJson(inJson, JobStatus.class);
                System.out.println("received TCP");
                socket.close();
            } catch (IOException e) {
                System.out.println("could not establish tcp socket connection");
                continue;
            }

            //check if we got a new job?
            if (newJob.getJob() != null)
                return newJob;
            try {
                Thread.sleep(durationBetweenPollMillis);
            } catch (InterruptedException e) {
                System.out.println("could not delay calls");
            }
        }
    }

    private static void finalizeJobWithServer(Worker worker) {
        BufferedReader inFromServer = null;
        DataOutputStream dataOutputStream = null;
        Gson gson = new Gson();
        Socket socket = null;
        JobStatus newJob = null;

        while (true) {
            try {
                //set up
                socket = new Socket(hostname, portNumber);
                inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                dataOutputStream = new DataOutputStream(socket.getOutputStream());


                System.out.println("sending tcp");
                dataOutputStream.writeBytes(worker.getCurrentJob().toJson() + "\n");
                System.out.println("received TCP");
                socket.close();
                break;
            } catch (IOException e) {
                System.out.println("could not establish tcp socket connection");
            }

            //try again
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
    }


    private static void printUsage() {
        System.out.println("Usage: java Client hostname port bucket workername ffmpeg ffprobe");
        System.out.println("         [hostname] : of tcp server");
        System.out.println("             [port] : of tcp server");
        System.out.println("           [bucket] : bucket name of s3");
        System.out.println("       [workername] : name you want to call your worker");
        System.out.println("           [ffmpeg] : path to ffmpeg");
        System.out.println("          [ffprobe] : path ti ffprobe");
    }
}
