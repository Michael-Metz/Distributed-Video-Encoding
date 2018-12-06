import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.*;

public class Server {


    private static Connection connection = null;

    public static void main(String args[]) throws Exception {

        //two many or too little arguments print the usage
        if (args.length != 5) {
            printUsage();
            System.exit(1);
        }

        int portNumber = 0;
        String argsPortNumber = args[0];
        String argsMysqlAddress = args[1];
        String argsMysqlDBName = args[2];
        String argsUsername = args[3];
        String argsPassword = args[4];

        //validate parameters
        try {
            portNumber = Integer.parseInt(args[0]);
        } catch (NumberFormatException exception) {
            System.out.println("ERR - arg 1");
            System.exit(1);
        }

        String dbPath = String.format("jdbc:mysql://%s/%s", argsMysqlAddress, argsMysqlDBName);
        //setup mysql
        try {
            connection = DriverManager.getConnection(dbPath, argsUsername, argsPassword);
        } catch (SQLException e) {
            System.out.print("could not connect to " + dbPath);
            e.printStackTrace();
        }


        ServerSocket socket = new ServerSocket(portNumber);
        Socket connSoc = null;
        InputStreamReader streamReader = null;
        BufferedReader connSocIn = null;
        Gson gson = new Gson();
        DataOutputStream outToClient = null;

        while (true) {
            System.out.println("Waiting for Connection");
            //accept the connection
            connSoc = socket.accept();
            System.out.println("Receiving TCP");

            //set up the input stream and output streams
            streamReader = new InputStreamReader(connSoc.getInputStream());
            connSocIn = new BufferedReader(streamReader);
            outToClient = new DataOutputStream(connSoc.getOutputStream());

            //read message from client
            String message = connSocIn.readLine();

            //validate that message is json
            JsonElement json = null;
            try {
                json = gson.fromJson(message, JsonElement.class);
            } catch (JsonSyntaxException e) {
                System.out.println("not json");
                continue;
            }

            JobStatus jobSatus = null;

            try {
                jobSatus = gson.fromJson(gson.toJson(json), JobStatus.class);
            } catch (JsonSyntaxException e) {
                System.out.println("We only accept json of type \'JobStatus\'");
                continue;
            }

            //client is sending a completed job so process it and then terminate the connect
            if (jobSatus.isComplete()) {
                System.out.println("----finalizing job from " + jobSatus.getWorkerSignature());
                finalizeJob(jobSatus);
                continue;
            }

            //if there is a new job lets attach it to the response
            if (isThereAnotherJob()) {
                System.out.println("----Fetching new job for " + jobSatus.getWorkerSignature());
                Job newJob = fetchNextJob();
                jobSatus.setJob(newJob);
                System.out.println("----send a new job to :" + jobSatus.getWorkerSignature());
                System.out.println("    |----" + jobSatus.getJob().toString());
            } else {
                System.out.println("----Sending empty job status to  " + jobSatus.getWorkerSignature());
            }

            outToClient.writeBytes(jobSatus.toJson());
            outToClient.close();
        }

    }

    private static void finalizeJob(JobStatus completedJobStatus) {
        Job job = completedJobStatus.getJob();

        //update to in progress
        String statement = "UPDATE job set inprogress=?, complete=?, workersignature=? where id=?";
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(statement);
            preparedStatement.setInt(1, 0);
            preparedStatement.setInt(2, 1);
            preparedStatement.setString(3, completedJobStatus.getWorkerSignature());
            preparedStatement.setLong(4, job.getId());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static boolean isThereAnotherJob() {

        try {
            String selectString = "SELECT * FROM job WHERE inprogress=0 AND complete=0 LIMIT 1";
            PreparedStatement selectStatement = connection.prepareStatement(selectString);
            ResultSet rs = null;
            rs = selectStatement.executeQuery();
            if (rs.first())
                return true;
            return false;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;

    }


    private static Job fetchNextJob() {
        Job job = new Job();
        try {
            String selectString = "SELECT * FROM job WHERE inprogress=0 AND complete=0 LIMIT 1";
            PreparedStatement selectStatement = connection.prepareStatement(selectString);
            ResultSet rs = selectStatement.executeQuery();
            rs.first();
            job.setId(rs.getLong("id"));
            job.setVideoid(rs.getLong("videoid"));
            job.setUploadid(rs.getLong("uploadid"));
            job.setOldvideofilename(rs.getString("oldvideofilename"));
            job.setNewvideofilename(rs.getString("newvideofilename"));
            job.setWidth(rs.getInt("width"));
            job.setHeight(rs.getInt("height"));
            job.setCrf(rs.getInt("crf"));
            job.setAudiobitrate(rs.getInt("audiobitrate"));
            job.setInprogress(rs.getBoolean("inprogress"));
            job.setComplete(rs.getBoolean("complete"));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //update to in progress
        String statement = "UPDATE job set inprogress=? where id=?";
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(statement);
            preparedStatement.setInt(1, 1);
            preparedStatement.setLong(2, job.getId());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return job;
    }

    private static void printUsage() {
        System.out.println("Usage: java Server port mysql-address db-name user pass");
        System.out.println("                [port] : Port number you want to run on,");
        System.out.println("                         Make sure it is open on your firewall");
        System.out.println("       [mysql-address] : the ip address to the mysql db");
        System.out.println("             [db-name] : the name of the database");
        System.out.println("                [user] : the username for mysql");
        System.out.println("                [pass] : the password for mysql");
    }

}
