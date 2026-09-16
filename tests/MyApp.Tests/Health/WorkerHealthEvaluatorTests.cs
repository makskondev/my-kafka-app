using Microsoft.Extensions.Diagnostics.HealthChecks;
using MyApp.Core.Health;
using Xunit;

namespace MyApp.Tests.Health;

public class WorkerHealthEvaluatorTests
{
    [Fact]
    public void Evaluate_ReturnsUnhealthy_WhenStateReportedFailure()
    {
        var state = new WorkerHealthState();
        state.ReportFailure("kafka broker unreachable");

        var result = WorkerHealthEvaluator.Evaluate(state, "OrderCreated", staleAfter: null);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("OrderCreated", result.Description);
        Assert.Contains("kafka broker unreachable", result.Description);
    }

    [Fact]
    public void Evaluate_ReturnsHealthy_WhenNoStalenessThresholdGiven()
    {
        // Для inbound-событий staleAfter = null: долгое отсутствие сообщений - это норма,
        // а не признак нездоровья.
        var state = new WorkerHealthState();

        var result = WorkerHealthEvaluator.Evaluate(state, "OrderCreated", staleAfter: null);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public void Evaluate_ReturnsHealthy_WhenLastActivityWithinThreshold()
    {
        var state = new WorkerHealthState();
        state.ReportSuccess();

        var result = WorkerHealthEvaluator.Evaluate(state, "InvoiceReady", TimeSpan.FromSeconds(30));

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public void Evaluate_ReturnsDegraded_WhenLastActivityOlderThanThreshold()
    {
        var state = new WorkerHealthState();
        state.ReportSuccess();
        Thread.Sleep(50); // гарантированно "старее" маленького порога ниже

        var result = WorkerHealthEvaluator.Evaluate(state, "InvoiceReady", TimeSpan.FromMilliseconds(10));

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("InvoiceReady", result.Description);
    }

    [Fact]
    public void Evaluate_UnhealthyTakesPrecedenceOverStaleness()
    {
        // Если сам вызов упал (IsHealthy=false), не важно, "устарела" активность или нет -
        // всегда Unhealthy, а не Degraded.
        var state = new WorkerHealthState();
        state.ReportFailure("boom");

        var result = WorkerHealthEvaluator.Evaluate(state, "InvoiceReady", TimeSpan.FromSeconds(1000));

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }
}
