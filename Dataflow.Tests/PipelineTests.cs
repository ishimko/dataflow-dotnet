using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Dataflow.Lib;
using NUnit.Framework;

namespace Dataflow.Tests
{
    public class PipelineTests
    {
        private string[] _files;

        [SetUp]
        public void Setup()
        {
            Cleanup();
            _files = Directory.GetFiles("TestData", "*.txt");
        }

        [TearDown]
        public void Cleanup()
        {
            foreach (string f in Directory.EnumerateFiles("TestData", "*.processed"))
            {
                File.Delete(f);
            }
        }

        [Test]
        public async Task MaxDegreeOfParallelismTest([Range(1, 10)]int maxParallelism)
        {
            var configuration = new PipelineConfiguration(maxReadingTasks: maxParallelism,
                maxProcessingTasks: maxParallelism,
                maxWritingTasks: maxParallelism);
            var pipeline = new Pipeline(configuration);
            await pipeline.PerformProcessing(_files);
            
            WriteSequence(pipeline.NumberOfReadingTasks, "Reading");
            WriteSequence(pipeline.NumberOfProcessingTasks, "Processing");
            WriteSequence(pipeline.NumberOfWritingTasks, "Writing");
            
            Assert.Multiple(() =>
            {
                Assert.That(pipeline.NumberOfWritingTasks, Is.All.AtMost(maxParallelism));
                Assert.That(pipeline.NumberOfReadingTasks, Is.All.AtMost(maxParallelism));
                Assert.That(pipeline.NumberOfProcessingTasks, Is.All.AtMost(maxParallelism));
            });
        }

        private static void WriteSequence(IEnumerable<int> sequence, string title)
        {
            Console.WriteLine($"{title}:");
            Console.WriteLine(string.Join(" ", sequence));
        }
    }
}