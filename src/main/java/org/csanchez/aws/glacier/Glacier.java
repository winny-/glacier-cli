package org.csanchez.aws.glacier;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.CreateVaultRequest;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.model.DeleteVaultRequest;
import com.amazonaws.services.glacier.model.DescribeVaultRequest;
import com.amazonaws.services.glacier.model.DescribeVaultResult;
import com.amazonaws.services.glacier.model.ListVaultsRequest;
import com.amazonaws.services.glacier.model.ListVaultsResult;
import com.amazonaws.services.glacier.model.DescribeVaultOutput;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

/**
 * A command line client to Amazon Glacier based on AWS examples.
 * http://aws.amazon.com/glacier
 * 
 * Uses Glacier high level API for uploading, downloading, deleting files, and
 * the low-level one for retrieving vault inventory.
 * 
 * More info at http://docs.amazonwebservices.com/amazonglacier/latest/dev/
 * 
 * @author Carlos Sanchez <a href="mailto:carlos@apache.org">
 */
public class Glacier {

    private static long sleepTime = 600;
    private AmazonGlacierClient client;
    private AmazonSQSClient sqsClient;
    private AmazonSNSClient snsClient;

    private AWSCredentials credentials;
    private String region;
    private static Options options;

    public Glacier(AWSCredentials credentials, String region) {
        this.credentials = credentials;
        this.region = region;
        client = new AmazonGlacierClient(credentials);
        client.setEndpoint(String.format("https://glacier.%s.amazonaws.com/", region));
    }

    public static void main(String[] args) throws Exception {
        options = commonOptions();

        try {
            CommandLineParser parser = new PosixParser();
            CommandLine cmd = parser.parse(options, args);
            List<String> arguments = Arrays.asList(cmd.getArgs());

            if (cmd.hasOption("help")) {
                usage();
                // Not reached
            }

            if (arguments.isEmpty()) {
                throw new GlacierCliException("Must provide at least one command.");
            }

            String commandName = arguments.get(0);
            arguments = arguments.subList(1, arguments.size());
            GlacierCliCommand command = GlacierCliCommand.get(commandName);
            if (command == null) {
                throw new GlacierCliException("Invalid command given: " + commandName);
            }

            String propertiesArugment = cmd.getOptionValue("properties");
            File props = (propertiesArugment != null) ? 
                new File(propertiesArugment) :
                new File(System.getProperty("user.home"), "AwsCredentials.properties");
            AWSCredentials credentials = new PropertiesCredentials(props);
            Glacier glacier = new Glacier(credentials, cmd.getOptionValue("region", "us-east-1"));

            switch (command) {

                // Archive commands
                case UPLOAD:
                    if (arguments.size() < 2) {
                        throw new GlacierCliException("The upload command requires at least two parameters.");
                    }
                    String vaultName = arguments.get(0);
                    for (String archive : arguments.subList(1, arguments.size())) {
                        glacier.upload(vaultName, archive);
                    }
                    break;

                case DELETE:
                    if (arguments.size() != 2) {
                        throw new GlacierCliException("The delete command requires exactly two parameters.");
                    }
                    glacier.delete(arguments.get(0), arguments.get(1));
                    break;

                case DOWNLOAD:
                    if (arguments.size() != 3) {
                        throw new GlacierCliException("The download command requires exactly three parameters.");
                    }
                    glacier.download(arguments.get(0), arguments.get(1), arguments.get(2));
                    break;

                // Vault commands
                case CREATE_VAULT:
                    if (arguments.size() != 1) {
                        throw new GlacierCliException("The create-vault command requires exactly one parameter.");
                    }
                    glacier.createVault(arguments.get(0));
                    break;

                case DELETE_VAULT:
                    if (arguments.size() != 1) {
                        throw new GlacierCliException("The delete-vault command requires exactly one parameters.");
                    }
                    glacier.deleteVault(arguments.get(0));
                    break;

                case INVENTORY:
                    if (arguments.size() != 1) {
                        throw new GlacierCliException("The inventory command requires exactly one parameter.");
                    }
                    glacier.inventory(
                        arguments.get(0),
                        cmd.getOptionValue("topic", "glacier"),
                        cmd.getOptionValue("queue", "glacier"),
                        cmd.getOptionValue("file", "glacier.json")
                    );
                    break;

                case INFO:
                    if (arguments.size() != 1) {
                        throw new GlacierCliException("The info command requires exactly one parameter.");
                    }
                    glacier.info(arguments.get(0));
                    break;

                case LIST:
                    glacier.list();
                    break;
            }
        } catch (GlacierCliException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println();

            usage();
        } catch (UnrecognizedOptionException e) {
            System.err.println("error: Invalid argument: " + e.getOption());
            usage();
        }
    }

    private static void usage() {
        PrintWriter out = new PrintWriter(System.err);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(out,
                            formatter.getWidth(),
                            "glacier " + "upload vault_name file1 file2 ... | "
                                       + "delete vault_name archiveId | "
                                       + "download vault_name archiveId output_file | "
                                       + "create-vault vault_name | "
                                       + "delete-vault vault_name | "
                                       + "inventory vault_name | "
                                       + "list vault_name | "
                                       + "info vault_name",
                            null,
                            options,
                            formatter.getLeftPadding(),
                            formatter.getDescPadding(),
                            null);
        out.flush();
        System.exit(1);
    }

    private static Options commonOptions() {
        Options options = new Options();

        Option properties = OptionBuilder.withArgName("properties").hasArg()
                .withDescription("Path to an AWSCredentials properties file. Defaults to '~/AwsCredentials.properties'").create("properties");
        options.addOption(properties);

        Option region = OptionBuilder.withArgName("region").hasArg()
                .withDescription("Specify URL as the web service URL to use. Defaults to 'us-east-1'").create("region");
        options.addOption(region);

        Option topic = OptionBuilder.withArgName("topic_name").hasArg()
                .withDescription("SNS topic to use for inventory retrieval. Defaults to 'glacier'").create("topic");
        options.addOption(topic);

        Option queue = OptionBuilder.withArgName("queue_name").hasArg()
                .withDescription("SQS queue to use for inventory retrieval. Defaults to 'glacier'").create("queue");
        options.addOption(queue);

        Option output = OptionBuilder.withArgName("file_name").hasArg()
                .withDescription("File to save the inventory to. Defaults to 'glacier.json'").create("output");
        options.addOption(output);

        Option help = OptionBuilder.withArgName("help")
                .withDescription("Show help information").create("help");
        options.addOption(help);

        return options;
    }

    // ================
    // Archive commands
    // ================

    public void upload(String vaultName, String archive) {
        String msg = String.format("Uploading %s to Glacier vault %s", archive, vaultName);
        System.out.println(msg);

        try {
            ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
            UploadResult result = atm.upload(vaultName, archive, new File(archive));
            System.out.println(String.format("Uploaded %s: %s", archive, result.getArchiveId()));
        } catch (Exception e) {
            throw new RuntimeException("Error " + msg, e);
        }
    }

    public void delete(String vaultName, String archiveId) {
        String msg = String.format("Deleting %s from Glacier vault %s", archiveId, vaultName);
        System.out.println(msg);

        try {
            client.deleteArchive(new DeleteArchiveRequest().withVaultName(vaultName).withArchiveId(archiveId));
            System.out.println("Deleted archive: " + archiveId);
        } catch (Exception e) {
            throw new RuntimeException("Error " + msg, e);
        }
    }

    public void download(String vaultName, String archiveId, String downloadFilePath) {
        String msg = String.format("Downloading %s from Glacier vault %s", archiveId, vaultName);
        System.out.println(msg);

        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint(String.format("https://sqs.%s.amazonaws.com", region));
        snsClient = new AmazonSNSClient(credentials);
        snsClient.setEndpoint(String.format("https://sns.%s.amazonaws.com", region));

        try {
            ArchiveTransferManager atm = new ArchiveTransferManager(client, sqsClient, snsClient);
            atm.download(vaultName, archiveId, new File(downloadFilePath));
        } catch (Exception e) {
            throw new RuntimeException("Error " + msg, e);
        }
    }

    // ==============
    // Vault commands
    // ==============

    public void createVault(String vaultName) {
        String msg = String.format("Creating vault \"%s\" ...", vaultName);
        System.out.println(msg);

        try {
            client.createVault(new CreateVaultRequest(vaultName));
            System.out.println(String.format("Created vault: \"%s\"", vaultName));
        } catch (Exception e) {
            throw new RuntimeException("Error " + msg, e);
        }
    }

    public void deleteVault (String vaultName) {
        String msg = "Deleting vault " + vaultName;
        System.out.println(msg);

        try {
            client.deleteVault(new DeleteVaultRequest(vaultName));
            System.out.println("Deleted vault: " + vaultName);
        } catch (Exception e) {
            throw new RuntimeException("Error " + msg, e);
        }
    }

    public void inventory(String vaultName, String snsTopicName, String sqsQueueName, String fileName) {
        String msg = "Requesting inventory from Glacier vault " + vaultName + ". It might take around 4 hours.";
        System.out.println(msg);

        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint(String.format("https://sqs.%s.amazonaws.com", region));
        snsClient = new AmazonSNSClient(credentials);
        snsClient.setEndpoint(String.format("https://sns.%s.amazonaws.com", region));

        try {
            QueueConfig config = setupSQS(sqsQueueName);

            config = setupSNS(config, snsTopicName);

            String jobId = initiateJobRequest(vaultName, config.snsTopicARN);
            System.out.println("Jobid = " + jobId);

            Boolean success = waitForJobToComplete(jobId, config.sqsQueueURL);
            if (!success) {
                throw new Exception("Job did not complete successfully.");
            }

            downloadJobOutput(vaultName, jobId, fileName);

            cleanUp(config);

        } catch (Exception e) {
            System.err.println("Archive retrieval failed.");
            System.err.println(e);
        }
    }

    public void info(String vaultName) {
        String msg = String.format("Info %s...", vaultName);
        System.out.println(msg);

        try {
            DescribeVaultResult vaultdescribe = client.describeVault(new DescribeVaultRequest(vaultName));
            System.out.print(makeVaultDescriptionString(vaultdescribe));
        } catch (Exception e) {
            throw new RuntimeException("Error " + msg, e);
        }
    }

    public void list() {
        String msg = "Listing ...";
        System.out.println(msg);

        try {
            ListVaultsResult vaults = client.listVaults(new ListVaultsRequest());

            for (DescribeVaultOutput vault: vaults.getVaultList())
            {
                System.out.println("----");
                System.out.print(makeVaultDescriptionString(vault));
            }

        } catch (Exception e) {
            throw new RuntimeException("Error " + msg, e);
        }
    }

    // ==============
    // Helper methods
    // ==============

    private QueueConfig setupSQS(String sqsQueueName) {
        QueueConfig config = new QueueConfig();
        CreateQueueRequest request = new CreateQueueRequest().withQueueName(sqsQueueName);
        CreateQueueResult result = sqsClient.createQueue(request);
        config.sqsQueueURL = result.getQueueUrl();

        GetQueueAttributesRequest qRequest = new GetQueueAttributesRequest().withQueueUrl(config.sqsQueueURL)
                .withAttributeNames("QueueArn");

        GetQueueAttributesResult qResult = sqsClient.getQueueAttributes(qRequest);
        config.sqsQueueARN = qResult.getAttributes().get("QueueArn");

        Policy sqsPolicy = new Policy().withStatements(new Statement(Effect.Allow).withPrincipals(Principal.AllUsers)
                .withActions(SQSActions.SendMessage).withResources(new Resource(config.sqsQueueARN)));
        Map<String, String> queueAttributes = new HashMap<String, String>();
        queueAttributes.put("Policy", sqsPolicy.toJson());
        sqsClient.setQueueAttributes(new SetQueueAttributesRequest(config.sqsQueueURL, queueAttributes));

        return config;
    }

    private QueueConfig setupSNS(QueueConfig config, String snsTopicName) {
        CreateTopicRequest request = new CreateTopicRequest().withName(snsTopicName);
        CreateTopicResult result = snsClient.createTopic(request);
        config.snsTopicARN = result.getTopicArn();

        SubscribeRequest request2 = new SubscribeRequest().withTopicArn(config.snsTopicARN).withEndpoint(config.sqsQueueARN)
                .withProtocol("sqs");
        SubscribeResult result2 = snsClient.subscribe(request2);

        config.snsSubscriptionARN = result2.getSubscriptionArn();

        return config;
    }

    private String initiateJobRequest(String vaultName, String snsTopicARN) {
        JobParameters jobParameters = new JobParameters().withType("inventory-retrieval").withSNSTopic(snsTopicARN);

        InitiateJobRequest request = new InitiateJobRequest().withVaultName(vaultName).withJobParameters(jobParameters);

        InitiateJobResult response = client.initiateJob(request);

        return response.getJobId();
    }

    private Boolean waitForJobToComplete(String jobId, String sqsQueueUrl) throws InterruptedException, JsonParseException, IOException {
        Boolean messageFound = false;
        Boolean jobSuccessful = false;
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getJsonFactory();
        Date start = new Date();

        while (!messageFound) {
            long minutes = (new Date().getTime() - start.getTime()) / 1000 / 60;
            System.out.println(String.format("Checking for messages: %d elapsed", minutes));
            List<Message> msgs = sqsClient.receiveMessage(new ReceiveMessageRequest(sqsQueueUrl).withMaxNumberOfMessages(10)).getMessages();

            if (msgs.size() > 0) {
                for (Message m : msgs) {
                    JsonParser jpMessage = factory.createJsonParser(m.getBody());
                    JsonNode jobMessageNode = mapper.readTree(jpMessage);
                    String jobMessage = jobMessageNode.get("Message").getTextValue();

                    JsonParser jpDesc = factory.createJsonParser(jobMessage);
                    JsonNode jobDescNode = mapper.readTree(jpDesc);
                    String retrievedJobId = jobDescNode.get("JobId").getTextValue();
                    String statusCode = jobDescNode.get("StatusCode").getTextValue();
                    if (retrievedJobId.equals(jobId)) {
                        messageFound = true;
                        if (statusCode.equals("Succeeded")) {
                            jobSuccessful = true;
                        }
                    }
                }

            } else {
                Thread.sleep(sleepTime * 1000);
            }
        }
        return (messageFound && jobSuccessful);
    }

    private void downloadJobOutput(String vaultName, String jobId, String fileName) throws IOException {
        GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest().withVaultName(vaultName).withJobId(jobId);
        GetJobOutputResult getJobOutputResult = client.getJobOutput(getJobOutputRequest);

        FileWriter fstream = new FileWriter(fileName);
        BufferedWriter out = new BufferedWriter(fstream);
        BufferedReader in = new BufferedReader(new InputStreamReader(getJobOutputResult.getBody()));
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            out.write(inputLine);
        }
        out.close();
        System.out.println("Retrieved inventory to " + fileName);
    }

    private void cleanUp(QueueConfig config) {
        snsClient.unsubscribe(new UnsubscribeRequest(config.snsSubscriptionARN));
        snsClient.deleteTopic(new DeleteTopicRequest(config.snsTopicARN));
        sqsClient.deleteQueue(new DeleteQueueRequest(config.sqsQueueURL));
    }

    private String makeVaultDescriptionString(DescribeVaultResult vault) {
        String sep = System.getProperty("line.separator");
        return String.format(
            "Vault : %s%s" +
            "\tCreation date : %s%s" +
            "\tNumber of archives : %s%s" +
            "\tVault size : %s%s" +
            "\tLast inventory date : %s%s",
            vault.getVaultName(), sep,
            vault.getCreationDate(), sep,
            vault.getNumberOfArchives(), sep,
            vault.getSizeInBytes(), sep,
            vault.getLastInventoryDate(), sep
        );
    }

    private String makeVaultDescriptionString(DescribeVaultOutput vault) {
        String sep = System.getProperty("line.separator");
        return String.format(
            "Vault : %s%s" +
            "\tCreation date : %s%s" +
            "\tNumber of archives : %s%s" +
            "\tVault size : %s%s" +
            "\tLast inventory date : %s%s",
            vault.getVaultName(), sep,
            vault.getCreationDate(), sep,
            vault.getNumberOfArchives(), sep,
            vault.getSizeInBytes(), sep,
            vault.getLastInventoryDate(), sep
        );
    }

    class QueueConfig {
        String sqsQueueURL, sqsQueueARN, snsTopicARN, snsSubscriptionARN;
    }
}
