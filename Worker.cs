using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace awssnsworker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Starting worker running at: {time}", DateTimeOffset.Now);

            //Reading configuration
            var aswSection = _configuration.GetSection("Aws");
            var accessKey = aswSection.GetSection("AccessKey").Value;
            var secretKey = aswSection.GetSection("SecretKey").Value;
            var sqsUrl = aswSection.GetSection("SQSUrl").Value;
            
            //Creating sqs client
            var credentials = new Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey);
            AmazonSQSClient amazonSQSClient = new AmazonSQSClient(credentials, Amazon.RegionEndpoint.SAEast1);

            /* teste envio
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.QueueUrl = sqsUrl;
            sendMessageRequest.MessageBody = "aha";
            var resposta = await amazonSQSClient.SendMessageAsync(sendMessageRequest);
            */
                        
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                //Receive request
                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsUrl);

                var response = await amazonSQSClient.ReceiveMessageAsync(receiveMessageRequest, stoppingToken);

                if (response.Messages.Any())
                {
                    foreach (Message message in response.Messages)
                    {
                        Console.WriteLine($"Message received");
                        Console.WriteLine($"Message: {message.Body}");

                        //Deleting message
                        var deleteMessageRequest = new DeleteMessageRequest(sqsUrl, message.ReceiptHandle);
                        await amazonSQSClient.DeleteMessageAsync(deleteMessageRequest, stoppingToken);

                        Console.WriteLine($"Message deleted");
                    }
                }

                await Task.Delay(5000, stoppingToken);
            }
        }
    }
}
