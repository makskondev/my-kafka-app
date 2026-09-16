using MyApp.Core.Health;
using Xunit;

namespace MyApp.Tests.Health;

public class WorkerHealthStateTests
{
    [Fact]
    public void InitialState_IsHealthyWithNoError()
    {
        var state = new WorkerHealthState();

        Assert.True(state.IsHealthy);
        Assert.Null(state.LastError);
    }

    [Fact]
    public void ReportFailure_MarksUnhealthyAndStoresError()
    {
        var state = new WorkerHealthState();

        state.ReportFailure("oracle down");

        Assert.False(state.IsHealthy);
        Assert.Equal("oracle down", state.LastError);
    }

    [Fact]
    public void ReportSuccess_AfterFailure_ClearsErrorAndMarksHealthyAgain()
    {
        var state = new WorkerHealthState();
        state.ReportFailure("oracle down");

        state.ReportSuccess();

        Assert.True(state.IsHealthy);
        Assert.Null(state.LastError);
    }

    [Fact]
    public void ReportSuccess_UpdatesLastActivityUtc()
    {
        var state = new WorkerHealthState();
        var before = DateTimeOffset.UtcNow;

        state.ReportSuccess();

        Assert.True(state.LastActivityUtc >= before);
    }
}
