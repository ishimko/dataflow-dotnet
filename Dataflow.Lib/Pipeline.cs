using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Dataflow.Lib
{
    public class Pipeline
    {
        private volatile int _readingCount;

        private volatile int _processingCount;

        private volatile int _writingCount;
        
        private readonly PipelineConfiguration _pipelineConfiguration;
        
        public ConcurrentBag<int> NumberOfReadingTasks { get; }

        public ConcurrentBag<int> NumberOfProcessingTasks { get; }
        
        public ConcurrentBag<int> NumberOfWritingTasks { get; }

        public Pipeline(PipelineConfiguration pipelineConfiguration)
        {
            _pipelineConfiguration = pipelineConfiguration;
            NumberOfReadingTasks = new ConcurrentBag<int>();
            NumberOfProcessingTasks = new ConcurrentBag<int>();
            NumberOfWritingTasks = new ConcurrentBag<int>();
        }

        public async Task PerformProcessing(IEnumerable<string> files)
        {
            _readingCount = 0;
            _processingCount = 0;
            _writingCount = 0;
            NumberOfProcessingTasks.Clear();
            NumberOfProcessingTasks.Clear();
            NumberOfWritingTasks.Clear();
            
            var linkOptions = new DataflowLinkOptions {PropagateCompletion = true};
            var readingBlock = new TransformBlock<string, FileWithContent>(
                async path => new FileWithContent(path, await ReadFile(path)),
                new ExecutionDataflowBlockOptions{MaxDegreeOfParallelism = _pipelineConfiguration.MaxReadingTasks});
            var processingBlock = new TransformBlock<FileWithContent, FileWithContent>(
                fwc => new FileWithContent(fwc.Path + ".processed", ProcessFile(fwc.Content)),
                new ExecutionDataflowBlockOptions{MaxDegreeOfParallelism = _pipelineConfiguration.MaxProcessingTasks});
            var writingBlock = new ActionBlock<FileWithContent>(async fwc => await WriteFile(fwc),
                new ExecutionDataflowBlockOptions{MaxDegreeOfParallelism = _pipelineConfiguration.MaxWritingTasks});
            
            readingBlock.LinkTo(processingBlock, linkOptions);
            processingBlock.LinkTo(writingBlock, linkOptions);
            
            foreach (string file in files)
            {
                readingBlock.Post(file);
            }
            
            readingBlock.Complete();

            await writingBlock.Completion;
        }

        private async Task<string> ReadFile(string filePath)
        {
            Interlocked.Increment(ref _readingCount);
            NumberOfReadingTasks.Add(_readingCount);
            string result;
            using (var streamReader = new StreamReader(filePath))
            {
                result = await streamReader.ReadToEndAsync();
            }
            Interlocked.Decrement(ref _readingCount);
            return result;
        }

        private string ProcessFile(string fileContent)
        {
            Interlocked.Increment(ref _processingCount);
            NumberOfProcessingTasks.Add(_processingCount);
            string result = fileContent.ToUpper();
            Thread.Sleep(250); // simulate hard work
            Interlocked.Decrement(ref _processingCount);
            return result;
        }

        private async Task WriteFile(FileWithContent fileWithContent)
        {
            Interlocked.Increment(ref _writingCount);
            NumberOfWritingTasks.Add(_writingCount);

            using (var streamWriter = new StreamWriter(fileWithContent.Path))
            {
                await streamWriter.WriteAsync(fileWithContent.Content);
            }

            Interlocked.Decrement(ref _writingCount);
        }
    }
}
