using MyApp.Contracts;
using MyApp.Contracts.Inbound;
using MyApp.Data;
using Oracle.ManagedDataAccess.Client;

namespace MyApp.Handlers;

public sealed record OrderCreatedMessage(long OrderId, string CustomerId, string Status);

/// <summary>
/// Пример inbound-обработчика. MERGE делает запись идемпотентной:
/// повторная доставка того же OrderId после сбоя/ретрая offset не создаёт дубликат.
/// </summary>
[EventType("OrderCreated")]
public sealed class OrderCreatedHandler(OracleConnectionFactory connectionFactory)
    : IInboundEventHandler<OrderCreatedMessage>
{
    public async Task HandleAsync(OrderCreatedMessage message, InboundEventContext context, CancellationToken ct)
    {
        await using var connection = await connectionFactory.OpenAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            MERGE INTO ORDERS o
            USING (SELECT :orderId AS ORDER_ID FROM DUAL) src
            ON (o.ORDER_ID = src.ORDER_ID)
            WHEN NOT MATCHED THEN
              INSERT (ORDER_ID, CUSTOMER_ID, STATUS, CREATED_AT)
              VALUES (:orderId, :customerId, :status, SYSTIMESTAMP)
            WHEN MATCHED THEN
              UPDATE SET STATUS = :status
            """;

        command.Parameters.Add(new OracleParameter("orderId", message.OrderId));
        command.Parameters.Add(new OracleParameter("customerId", message.CustomerId));
        command.Parameters.Add(new OracleParameter("status", message.Status));

        await command.ExecuteNonQueryAsync(ct);
    }
}
