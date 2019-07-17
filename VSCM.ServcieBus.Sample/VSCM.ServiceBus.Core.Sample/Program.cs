using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using VSCM.MessagingProtocol;
using VSCM.ServiceBusDeadLetterReader;
using VSCM.ServiceBusReader;
using VSCM.ServiceBusReader.Enums;
using VSCM.ServiceBusWriter;

namespace VSCM.ServiceBus.Core.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            var app = new Sample();

            app.Run();
        }
    }

    class Sample
    {
        private readonly int breakdown = 100;

        readonly ConcurrentQueue<VSCM_ServiceBusMessage> _messages = new ConcurrentQueue<VSCM_ServiceBusMessage>();
        readonly ConcurrentQueue<VSCM_ServiceBusDeadLetterMessage> _deadLetterMessages = new ConcurrentQueue<VSCM_ServiceBusDeadLetterMessage>();

        public void Run()
        {
            var results = Menu();

            var file = results.Item1;
            var path = results.Item2;

            Console.Clear();
            Console.WriteLine("Writing contents of " + file + " to " + path);

            ReadFromServiceBus(path);

            //WriteToServiceBus(file, path);

            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        private Tuple<string, string> Menu()
        {
            var file = FileMenu();
            var path = PathMenu();

            return new Tuple<string, string>(file, path);
        }

        private string FileMenu()
        {
            Console.Clear();
            Console.WriteLine("Please select a file to load:");
            Console.WriteLine("1 - small");
            Console.WriteLine("2 - medium");
            Console.WriteLine("3 - large");
            Console.WriteLine();

            var fileInput = Console.ReadLine();

            int.TryParse(fileInput, out var fileResult);

            if (fileResult == 1)
            {
                return "small.csv";
            }
            if (fileResult == 2)
            {
                return "medium.csv";
            }
            if (fileResult == 3)
            {
                return "large.csv";
            }

            return FileMenu();
        }

        private string PathMenu()
        {
            Console.Clear();
            Console.WriteLine("Please select a path to write to:");
            Console.WriteLine("1 - Queue");
            Console.WriteLine("2 - Topic");
            Console.WriteLine();

            var pathInput = Console.ReadLine();

            int pathResult;

            int.TryParse(pathInput, out pathResult);

            if (pathResult == 1)
            {
                return "dontwritetothisqueue";
            }
            if (pathResult == 2)
            {
                return "dontwritetothistopic2";
            }

            return PathMenu();
        }

        private void ReadFromServiceBus(string path)
        {
            VSCM_ServiceBusReader reader;
            VSCM_ServiceBusDeadLetterReader deadLetterReader;

            if (path.Contains("queue"))
            {
                reader = new VSCM_ServiceBusReader(ConfigurationManager.AppSettings.Get("PrimaryConnectionString"), ConfigurationManager.AppSettings.Get("SecondaryConnectionString"), new TimeSpan(0, 0, 0, 15), new TimeSpan(0, 0, 5, 0), ThreadCount.Five, ConnectionCount.Twenty, path);

                reader.SynchronouslyReadFromServiceBus(ReceiveMessage, null);

                deadLetterReader = new VSCM_ServiceBusDeadLetterReader(ConfigurationManager.AppSettings.Get("PrimaryConnectionString"), ConfigurationManager.AppSettings.Get("SecondaryConnectionString"), new TimeSpan(0, 0, 0, 15), path);

                deadLetterReader.SynchronouslyReadFromServiceBus(ReceiveDeadLetterMessage);
            }
            else
            {
                var subs = new[]
                {
                    "sub4"
                };

                reader = new VSCM_ServiceBusReader(ConfigurationManager.AppSettings.Get("PrimaryConnectionString"), string.Empty, new TimeSpan(0, 0, 1, 0), new TimeSpan(0, 0, 5, 0), ThreadCount.Five, ConnectionCount.Twenty, path, subs[0]);

                //reader.SynchronouslyReadFromServiceBus(ReceiveMessage);

                reader.AsynchronouslyReadFromServiceBus(ReceiveMessage, null);

                deadLetterReader = new VSCM_ServiceBusDeadLetterReader(ConfigurationManager.AppSettings.Get("PrimaryConnectionString"), string.Empty, new TimeSpan(0, 0, 0, 15), path, subs[0]);

                deadLetterReader.SynchronouslyReadFromServiceBus(ReceiveDeadLetterMessage);
            }
        }

        private void WriteToServiceBus(string file, string path)
        {
            var lines = File.ReadAllLines("../../../Sample Files/" + file);

            var batchId = Guid.NewGuid().ToString();

            var messages = lines.Select(l => new VSCM_ServiceBusMessage { Template = "1-Sample", BatchId = batchId, Message = l }).ToList();

            var messagesList = new List<List<VSCM_ServiceBusMessage>>();

            var count = 0;
            var messageCount = messages.Count;

            while (count < messageCount)
            {
                var skip = count;

                messagesList.Add(messages.Skip(skip).Take(breakdown).ToList());

                count += breakdown;
            }

            var errorMessages = new List<ErrorMessage>();

            var primaryConnectionString = ConfigurationManager.AppSettings.Get("PrimaryConnectionString");
            var secondaryConnectionString = ConfigurationManager.AppSettings.Get("SecondaryConnectionString");
            var transferQueue = "sampleprojecttransferqueue";

            var writer = new VSCM_ServiceBusWriter(primaryConnectionString, secondaryConnectionString, path, transferQueue);

            var elapsedTime = TimeSpan.Zero;

            for (var i = 0; i < messagesList.Count; i++)
            {
                var loopSw = new Stopwatch();

                loopSw.Start();

                Console.WriteLine();
                Console.WriteLine("Writing " + (i * breakdown) + " - " + ((i * breakdown) + messagesList[i].Count) + " of " + messageCount);

                errorMessages.AddRange(writer.WriteMessagesToServiceBus(messagesList[i]));

                loopSw.Stop();

                Console.WriteLine("Elapsed time: " + loopSw.Elapsed.ToString("mm':'ss'.'fff"));
                Console.WriteLine("Message/sec: " + messagesList[i].Count / loopSw.Elapsed.TotalSeconds);

                elapsedTime += loopSw.Elapsed;
            }

            Console.WriteLine();
            Console.WriteLine("Error Count: " + errorMessages.Count);
            Console.WriteLine();
            Console.WriteLine("Total Lines Written: " + messages.Count);
            Console.WriteLine("Total Elapsed Time: " + elapsedTime.ToString("mm':'ss'.'fff"));
            Console.WriteLine("Average Message/sec: " + messageCount / elapsedTime.TotalSeconds);
        }

        private ServiceBusReader.Models.MessageReadResponse ReceiveMessage(object sender, VSCM_ServiceBusMessage message)
        {
            _messages.Enqueue(message);

            Console.WriteLine("Current Message Count: " + _messages.Count);

            //return new ServiceBusReader.Models.MessageReadResponse
            //{
            //    ProcessSuccessful = true
            //};

            return new ServiceBusReader.Models.MessageReadResponse
            {
                ProcessSuccessful = false,
                DeadLetterReason = "Failed processing",
                DeadLetterErrorDescription = "Test"
            };
        }

        private ServiceBusDeadLetterReader.Models.MessageReadResponse ReceiveDeadLetterMessage(object sender, VSCM_ServiceBusDeadLetterMessage message)
        {
            _deadLetterMessages.Enqueue(message);

            Console.WriteLine("Dead Letter Current Message Count: " + _deadLetterMessages.Count);

            return new ServiceBusDeadLetterReader.Models.MessageReadResponse
            {
                ProcessSuccessful = true
            };

            //return new VSCM.ServiceBusDeadLetterReader.MessageReadResponse
            //{
            //    ProcessSuccessful = false
            //};
        }
    }
}
