namespace Dataflow.Lib
{
    public class PipelineConfiguration
    {
        public int MaxReadingTasks { get; }

        public int MaxProcessingTasks { get; }

        public int MaxWritingTasks { get; }

        public PipelineConfiguration(int maxReadingTasks, int maxProcessingTasks, int maxWritingTasks)
        {
            MaxReadingTasks = maxReadingTasks;
            MaxProcessingTasks = maxProcessingTasks;
            MaxWritingTasks = maxWritingTasks;
        }
    }
}