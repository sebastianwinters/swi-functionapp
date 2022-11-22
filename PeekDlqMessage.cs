using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

namespace MonitoringPoc
{
    public static class PeekDlqMessage
    {
        private const string TopicName = "t-inbound";
        private const string SubscriptionName = "s-consumer-1";

        [FunctionName("PeekDlqMessage")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context)
        {
            long sequenceNumber = 0;
            if (!long.TryParse(req.Query["seqnr"], out sequenceNumber))
            {
                log.LogWarning("Missing or invalid seqnr; return bad request");
                return new BadRequestObjectResult("Missing or invalid seqnr");
            }

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();
            
            var sbClient = new ServiceBusClient(config["sbconnectionString"]);
            var receiver  = sbClient.CreateReceiver(TopicName, SubscriptionName, new ServiceBusReceiverOptions{ SubQueue = SubQueue.DeadLetter });
            var message = await receiver.PeekMessageAsync(sequenceNumber);
            if (message?.SequenceNumber != sequenceNumber)
            {
                log.LogWarning(message != null ? "No message with seqnr {sequenceNumber} found; return empty result" : "No message in queue; return empty result");
                return new EmptyResult();
            }

            log.LogInformation("Message with seqnr {sequenceNumber} found");
            return new ObjectResult(message);
        }
    }
}
