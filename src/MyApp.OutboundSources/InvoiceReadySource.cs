using System.Globalization;
using MyApp.Contracts;
using MyApp.Contracts.Outbound;
using MyApp.Data;
using Oracle.ManagedDataAccess.Client;

namespace MyApp.OutboundSources;

public sealed record InvoiceReadyMessage(decimal InvoiceId, string CustomerId, decimal Amount, string Currency);

/// <summary>
/// Пример outbound-источника. Имя view передаётся через registration.SourceView (из конфига),
/// поэтому один и тот же класс переиспользуем не хардкодя имя таблицы/view напрямую.
/// </summary>
[EventType("InvoiceReady")]
public sealed class InvoiceReadySource(
    OracleConnectionFactory connectionFactory,
    Core.Outbound.OutboundEventRegistration registration)
    : IOutboundEventSource<InvoiceReadyMessage>
{
    public async Task<IReadOnlyList<OutboundItem<InvoiceReadyMessage>>> FetchPendingAsync(CancellationToken ct)
    {
        var result = new List<OutboundItem<InvoiceReadyMessage>>();

        await using var connection = await connectionFactory.OpenAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT INVOICE_ID, CUSTOMER_ID, AMOUNT, CURRENCY FROM {registration.SourceView}";

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var id = reader.GetDecimal(0);
            result.Add(new OutboundItem<InvoiceReadyMessage>(
                IdempotencyKey: id.ToString(CultureInfo.InvariantCulture),
                Payload: new InvoiceReadyMessage(id, reader.GetString(1), reader.GetDecimal(2), reader.GetString(3))));
        }

        return result;
    }

    public IReadOnlyList<OracleProcedureParameter> GetSuccessParameters(OutboundItem<InvoiceReadyMessage> item) =>
    [
        new("invoiceId", item.Payload.InvoiceId, OracleDbType.Decimal),
        new("sentAt", DateTime.UtcNow, OracleDbType.TimeStamp),
    ];

    public IReadOnlyList<OracleProcedureParameter> GetErrorParameters(
        OutboundItem<InvoiceReadyMessage> item, ProcessingError error) =>
    [
        new("invoiceId", item.Payload.InvoiceId, OracleDbType.Decimal),
        new("errorMessage", Truncate(error.Reason, 4000), OracleDbType.Varchar2),
    ];

    private static string Truncate(string value, int maxLength)
        => value.Length > maxLength ? value[..maxLength] : value;
}
