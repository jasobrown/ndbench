package com.netflix.ndbench.plugin.aws_sql.operations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.aws_sql.configs.AwsSqlConfiguration;

@Singleton
@NdBenchClientPlugin("AwsSqlBillingPlugin")
public class AwsSqlBillingPlugin extends AwsSqlPluginBase {
    private static final Logger logger = LoggerFactory.getLogger(AwsSqlBillingPlugin.class);

    private static final int MAX_SUBSCRIBERS = 150_000_000;
    private static final AtomicInteger currentSubscriberId = new AtomicInteger();

    @Inject
    protected AwsSqlBillingPlugin(AwsSqlConfiguration awsSqlConfiguration) {
        super(awsSqlConfiguration);
    }

    /**
        1. create invoice
        - create a financial event
        - add a few random finEvent attrs, 0-4
        - add a status_change_event

        2. create invoice items - almost always just one, but maybe every 1 out of 100/1000 has 2?
        - add discount to 1 out of 10
        - always do period reset
        - *if* mop on invoice was ???, then do balance consumption
        - add a few random attrs, 0-4
     */
    @Override
    public String writeSingle(String key) throws Exception {
        String subscriberId = Integer.toString(currentSubscriberId.incrementAndGet() % MAX_SUBSCRIBERS);
        long nowInMillis = System.currentTimeMillis();
        Connection connection = readWritePool.getConnection();
        connection.setAutoCommit(false);
        try {
            insertInvoice(connection, key, subscriberId, nowInMillis);
            insertInvoiceItems(connection, key, subscriberId, nowInMillis);
            connection.commit();
            return ResultOK;
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.close();
        }
    }

    private void insertInvoice(Connection connection, String invoiceId, String subscriberId, long nowInMillis) throws SQLException {
        // first the main invoice table
        PreparedStatement invoiceInsertPS = getInvoiceInsertPS(connection);
        invoiceInsertPS.setString(1, invoiceId);
        invoiceInsertPS.setString(2, subscriberId);
        invoiceInsertPS.setString(3, "renewal");
        invoiceInsertPS.setString(4, "A-OK");
        invoiceInsertPS.setString(5, "A-OK");
        invoiceInsertPS.setString(6, "billing-app");
        invoiceInsertPS.setTimestamp(7, new Timestamp(nowInMillis));
        invoiceInsertPS.setString(8, "billing-app");
        invoiceInsertPS.setTimestamp(9, new Timestamp(nowInMillis));
        invoiceInsertPS.setString(10, invoiceId);

        invoiceInsertPS.setString(11, "95032");
        invoiceInsertPS.setString(12, "Cali");
        invoiceInsertPS.setString(13, "USA");
        invoiceInsertPS.setString(14, "-11");
        invoiceInsertPS.setString(15, null);
        invoiceInsertPS.setString(16, "1234567890");
        invoiceInsertPS.setString(17, invoiceId);
        invoiceInsertPS.setString(18, null);
        invoiceInsertPS.setLong(19, 42);
        invoiceInsertPS.setLong(20, 2016);

        invoiceInsertPS.setTimestamp(21, new Timestamp(nowInMillis));
        invoiceInsertPS.execute();

        // the financial_event table
        PreparedStatement financialEventInsertPS = getFinancialEventInsertPS(connection);
        String financialEventId = dataGenerator.getRandomString();
        financialEventInsertPS.setString(1, financialEventId);
        financialEventInsertPS.setString(2, subscriberId);
        financialEventInsertPS.setString(3, "billing");
        financialEventInsertPS.setString(4, "billing-app");
        financialEventInsertPS.setTimestamp(5, new Timestamp(nowInMillis));
        financialEventInsertPS.setTimestamp(6, new Timestamp(nowInMillis));
        financialEventInsertPS.setString(7, "credit-card-processor");
        financialEventInsertPS.setLong(8, 10);
        financialEventInsertPS.setLong(9, 1);
        financialEventInsertPS.setString(10, "usd");

        financialEventInsertPS.setString(11, "credit-card-co");
        financialEventInsertPS.setString(12, "1234");
        financialEventInsertPS.setString(13, "kjdf976wejbsdiuvy");
        financialEventInsertPS.setString(14, "card-processor");
        financialEventInsertPS.setString(15, "settler");
        financialEventInsertPS.setString(16, invoiceId);
        financialEventInsertPS.setString(17, "sub-type");
        financialEventInsertPS.setString(18, "type");
        financialEventInsertPS.setString(19, "method");
        financialEventInsertPS.setString(20, "10-10");
        financialEventInsertPS.execute();

        // the financial_event_attribute table
        int attrsCount = Math.abs(dataGenerator.getRandomInteger()) & 0x03;
        for (int i = 0; i < attrsCount; i++) {
            PreparedStatement financialEventAttrsInsertPS = getFinancialEvenAttrInsertPS(connection);
            financialEventAttrsInsertPS.setString(1, financialEventId);
            financialEventAttrsInsertPS.setString(2, Integer.toString(i));
            financialEventAttrsInsertPS.setString(3, invoiceId);
            financialEventAttrsInsertPS.execute();
        }

        // the status_change_event table
        if (Math.abs(dataGenerator.getRandomInteger()) % 64 == 0) {
            PreparedStatement statusChangeEventInsertPS = getStatusChangeEventInsertPS(connection);
            statusChangeEventInsertPS.setString(1, invoiceId);
            statusChangeEventInsertPS.setString(2, subscriberId);
            statusChangeEventInsertPS.setString(3, "billing-app");
            statusChangeEventInsertPS.setTimestamp(4, new Timestamp(nowInMillis));
            statusChangeEventInsertPS.setTimestamp(5, new Timestamp(nowInMillis));
            statusChangeEventInsertPS.setString(6, "billed");
            statusChangeEventInsertPS.setString(7, invoiceId);
            statusChangeEventInsertPS.execute();
        }
    }

    private void insertInvoiceItems(Connection connection, String invoiceId, String subscriberId, long nowInMillis) throws SQLException {
        String invoiceItemId = dataGenerator.getRandomString();

        // first the invoice_items table
        PreparedStatement invoiceItemInsertPS = getInvoiceItemInsertPS(connection);
        invoiceItemInsertPS.setString(1, invoiceItemId);
        invoiceItemInsertPS.setString(2, subscriberId);
        invoiceItemInsertPS.setString(3, "subscription-renewal");
        invoiceItemInsertPS.setString(4, "billing-app");
        invoiceItemInsertPS.setTimestamp(5, new Timestamp(nowInMillis));
        invoiceItemInsertPS.setString(6, "billing-app");
        invoiceItemInsertPS.setTimestamp(7, new Timestamp(nowInMillis));
        invoiceItemInsertPS.setString(8, subscriberId);
        invoiceItemInsertPS.setString(9, "standard-service");
        invoiceItemInsertPS.setLong(10, 42);

        invoiceItemInsertPS.setTimestamp(11, new Timestamp(nowInMillis));
        invoiceItemInsertPS.setTimestamp(12, new Timestamp(nowInMillis + 1));
        invoiceItemInsertPS.setLong(13, 10);
        invoiceItemInsertPS.setString(14, "usd");
        invoiceItemInsertPS.setLong(15, 10);
        invoiceItemInsertPS.setLong(16, 1);
        invoiceItemInsertPS.setDouble(17, 10);
        invoiceItemInsertPS.setString(18, "usd");
        invoiceItemInsertPS.setLong(19, 10);
        invoiceItemInsertPS.setLong(20, 1);

        invoiceItemInsertPS.setString(21, "usd");
        invoiceItemInsertPS.setString(22, invoiceId);
        invoiceItemInsertPS.setTimestamp(23, new Timestamp(nowInMillis));
        invoiceItemInsertPS.setString(24, invoiceId);
        invoiceItemInsertPS.setString(25, "none");
        invoiceItemInsertPS.setString(26, "none");
        invoiceItemInsertPS.setString(27, "none");
        invoiceItemInsertPS.execute();

        // the invoice_items_attribute table
        int attrsCount = Math.abs(dataGenerator.getRandomInteger()) & 0x03;
        for (int i = 0; i < attrsCount; i++) {
            PreparedStatement invoiceItemAttrInsertPS = getInvoiceItemAttrInsertPS(connection);
            invoiceItemAttrInsertPS.setString(1, invoiceItemId);
            invoiceItemAttrInsertPS.setString(2, Integer.toString(i));
            invoiceItemAttrInsertPS.setString(3, invoiceId);
            invoiceItemAttrInsertPS.execute();
        }

        // the period_reset table
        PreparedStatement periodResetInsertPS = getPeriodResetInsertPS(connection);
        periodResetInsertPS.setString(1, invoiceItemId);
        periodResetInsertPS.setTimestamp(2, new Timestamp(nowInMillis));
        periodResetInsertPS.setTimestamp(3, new Timestamp(nowInMillis));
        periodResetInsertPS.setTimestamp(4, new Timestamp(nowInMillis + 1));
        periodResetInsertPS.setString(5, "good-reason");
        periodResetInsertPS.setString(6, "billing-app");
        periodResetInsertPS.setString(7, invoiceItemId);
        periodResetInsertPS.setString(8, "none");
        periodResetInsertPS.execute();

        // the discount table
        if (Math.abs(dataGenerator.getRandomInteger()) % 100 == 0) {
            PreparedStatement discountInsertPS = getDiscountInsertPS(connection);
            discountInsertPS.setString(1, invoiceItemId);
            discountInsertPS.setString(2, "big-big-discount");
            discountInsertPS.setLong(3, 1);
            discountInsertPS.setString(4, "usd");
            discountInsertPS.setLong(5, 42);
            discountInsertPS.setString(6, invoiceItemId);
            discountInsertPS.execute();
        }

        // the balance_consumption table
        if (Math.abs(dataGenerator.getRandomInteger()) % 100 == 0) {
            PreparedStatement balanceConsumptionInsertPS = getBalanceConsumptionInsertPS(connection);
            balanceConsumptionInsertPS.setString(1, invoiceItemId);
            balanceConsumptionInsertPS.setString(2, "gift-card");
            balanceConsumptionInsertPS.setString(3, invoiceId);
            balanceConsumptionInsertPS.setLong(4, 1);
            balanceConsumptionInsertPS.setString(5, "usd");
            balanceConsumptionInsertPS.setTimestamp(6, new Timestamp(nowInMillis));
            balanceConsumptionInsertPS.setString(7, "efshdgf98wtef");
            balanceConsumptionInsertPS.setString(8, "ref-id");
            balanceConsumptionInsertPS.setString(9, invoiceItemId);
            balanceConsumptionInsertPS.setString(10, "billing-app");
            balanceConsumptionInsertPS.setTimestamp(11, new Timestamp(nowInMillis));
            balanceConsumptionInsertPS.execute();
        }
    }

    @Override
    public String readSingle(String key) throws Exception
    {
        return null;
    }

    @Override
    public void createTables() throws Exception
    {
        try (Connection connection = readWritePool.getConnection())
        {
            createInvoiceTables(connection);
            createInvoiceItemsTables(connection);

        }
    }

    private void createInvoiceTables(Connection connection) throws Exception
    {
        // FIRST, the primary invoice table
        String invoice = "create table if not exists invoice " +
                         "( " +
                         "id varchar(255) not null, " +
                         "customer_id varchar(255) not null, " +
                         "transaction_type varchar(255) null, " +
                         "status varchar(255) null, " +
                         "transaction_processing_status varchar(255) null, " +
                         "created_by varchar(255) null, " +
                         "creation_timestamp timestamp(3) null, " +
                         "updated_by varchar(255) null, " +
                         "update_timestamp timestamp(3) null, " +
                         "billing_entity varchar(255) null, " +
                         "billing_zip varchar(255) null, " +
                         "region varchar(255) null, " +
                         "country varchar(255) null, " +
                         "time_zone int null, " +
                         "legacy_order_id varchar(255) null, " +
                         "payment_reference_id varchar(255) null, " +
                         "source_invoice_id varchar(255) null, " +
                         "source_legacy_order_id varchar(255) null, " +
                         "version bigint null, " +
                         "sql_version bigint null, " +
                         "resubmit_timestamp timestamp(3) null, " +
                         "primary key (customer_id, id) " +
                         ")";
        connection.createStatement().execute(invoice);
        try {
            connection.createStatement().execute("create index invoice_status_index on invoice (status)");
            connection.createStatement().execute("create index invoice_transaction_processing_status_index on invoice (transaction_processing_status)");
            connection.createStatement().execute("create index invoice_creation_timestamp_index on invoice (creation_timestamp)");
        } catch (Exception e) {
            // ignore - assume this is because the indices already exist
            // mysql doesn't have a "create index if nor exists" option
        }

        // now the status change event table
        String statusChange = "create table if not exists status_change_event " +
                              "( " +
                              "id varchar(255) not null " +
                              "primary key, " +
                              "customer_id varchar(255) null, " +
                              "created_by varchar(255) null, " +
                              "creation_timestamp timestamp(3) null, " +
                              "event_timestamp timestamp(3) null, " +
                              "status_change_reason varchar(255) null, " +
                              "invoice_id varchar(255) null, " +
                              "constraint fkeh0cmdm10ixk4lmuot0ankixs " +
                              "foreign key (customer_id, invoice_id) references invoice (customer_id, id) " +
                              ")";
        connection.createStatement().execute(statusChange);
        try {
            connection.createStatement().execute("create index fkeh0cmdm10ixk4lmuot0ankixs on status_change_event (customer_id, invoice_id)");
        } catch (Exception e) {
        }

        // now the financial event table
        String financialEvent = "create table if not exists financial_event " +
                                 "( " +
                                 " id varchar(255) not null " +
                                 "  primary key, " +
                                 " customer_id varchar(255) null, " +
                                 " financial_event_type varchar(255) null, " +
                                 " created_by varchar(255) null, " +
                                 " creation_timestamp timestamp(3) null, " +
                                 " event_timestamp timestamp(3) null, " +
                                 " billing_entity varchar(255) null, " +
                                 " amount_before_tax bigint null, " +
                                 " tax bigint null, " +
                                 " currency varchar(255) null, " +
                                 " card_type varchar(255) null, " +
                                 " last_four varchar(255) null, " +
                                 " mop_id varchar(255) null, " +
                                 " payment_processor varchar(255) null, " +
                                 " settlement_entity varchar(255) null, " +
                                 " invoice_id varchar(255) null, " +
                                 " vault_mop_sub_type varchar(255) null, " +
                                 " vault_mop_type varchar(255) null, " +
                                 " processing_method varchar(255) null, " +
                                 " item_to_amount varchar(1024) null " +
                                 ") ";
        connection.createStatement().execute(financialEvent);
        try {
            connection.createStatement().execute("create index fkrcij89tuxluio483x04kr8ao0 on financial_event (customer_id, invoice_id)");
            connection.createStatement().execute("alter table financial_event add constraint fkrcij89tuxluio483x04kr8ao0 " +
                                                 "foreign key (customer_id, invoice_id) references invoice (customer_id, id)");
        } catch (Exception e) {
        }

        String financialEventAttribute = "create table if not exists financial_event_attribute " +
                                         "( " +
                                         " id varchar(255) not null, " +
                                         " name varchar(255) not null, " +
                                         " value varchar(255) null, " +
                                         " primary key (id, name), " +
                                         " constraint fk8l05gtk46i1ku6ighj2yfjg8o " +
                                         "  foreign key (id) references financial_event (id) " +
                                         ") ";
        connection.createStatement().execute(financialEventAttribute);
    }

    private void createInvoiceItemsTables(Connection connection) throws Exception
    {
        // FIRST the invoice item table
        String invoiceItem = "create table if not exists invoice_item " +
                             "( " +
                             " id varchar(255) not null " +
                             "  primary key, " +
                             " customer_id varchar(255) null, " +
                             " item_type varchar(31) not null, " +
                             " created_by varchar(255) null, " +
                             " creation_timestamp timestamp(3) null, " +
                             " updated_by varchar(255) null, " +
                             " update_timestamp timestamp(3) null, " +
                             " subscription_id varchar(255) null, " +
                             " service_type varchar(255) null, " +
                             " period_num bigint null, " +
                             " period_start timestamp(3) null, " +
                             " period_end timestamp(3) null, " +
                             " item_price bigint null, " +
                             " item_price_currency varchar(255) null, " +
                             " selling_price_before_tax bigint null, " +
                             " selling_price_tax bigint null, " +
                             " tax_percent double null, " +
                             " selling_price_currency varchar(255) null, " +
                             " received_amount_before_tax bigint null, " +
                             " received_amount_tax bigint null, " +
                             " received_amount_currency varchar(255) null, " +
                             " invoice_id varchar(255) null, " +
                             " subscription_start timestamp(3) null, " +
                             " source_invoice_item_id varchar(255) null, " +
                             " issue_credit_type varchar(255) null, " +
                             " plan_change_type varchar(255) null, " +
                             " plan_change_id varchar(255) null, " +
                             " constraint fki2047w1t5njq30x7v8x96qq29 " +
                             " foreign key (customer_id, invoice_id) references invoice (customer_id, id) " +
                             ")";
        connection.createStatement().execute(invoiceItem);
        try {
            connection.createStatement().execute("create index fki2047w1t5njq30x7v8x96qq29 on invoice_item (customer_id, invoice_id)");
        } catch (Exception e) {
        }

        // next the invoice item attr table
        String invoiceItemAttribute = "create table if not exists invoice_item_attribute " +
                                      "( " +
                                      " id varchar(255) not null, " +
                                      " name varchar(255) not null, " +
                                      " value varchar(255) null, " +
                                      " primary key (id, name), " +
                                      " constraint fk1g3i5cl3jwp24uecgsx1lujph " +
                                      " foreign key (id) references invoice_item (id) " +
                                      ") ";
        connection.createStatement().execute(invoiceItemAttribute);

        // next the balance consumption table
        String balanceConsuption = "create table if not exists balance_consumption " +
                                   "( " +
                                   " id varchar(255) not null " +
                                   "  primary key, " +
                                   " balance_type varchar(255) null, " +
                                   " consumption_invoice_id varchar(255) null, " +
                                   " amount bigint null, " +
                                   " currency varchar(255) null, " +
                                   " redemption_date datetime null, " +
                                   " redemption_id varchar(255) null, " +
                                   " reference_id varchar(255) null, " +
                                   " invoice_item_id varchar(255) null, " +
                                   " created_by varchar(255) null, " +
                                   " creation_timestamp timestamp(3) null " +
                                   ") ";
        connection.createStatement().execute(balanceConsuption);
        try {
            connection.createStatement().execute("create index fkpcwj2wer69meod3n6um84a5f8 on balance_consumption (invoice_item_id)");
            connection.createStatement().execute("create index balance_consumption_creation_idx on balance_consumption (creation_timestamp)");
            connection.createStatement().execute("alter table balance_consumption add constraint fkpcwj2wer69meod3n6um84a5f8 " +
                                                 "foreign key (invoice_item_id) references invoice_item (id)");
        } catch (Exception e) {
        }

        // next the discount table
        String discount = "create table if not exists discount " +
                          "( " +
                          " id varchar(255) not null " +
                          "  primary key, " +
                          " discount_type varchar(255) null, " +
                          " amount bigint null, " +
                          " currency varchar(255) null, " +
                          " discount_id varchar(255) null, " +
                          " invoice_item_id varchar(255) null " +
                          ") ";
        connection.createStatement().execute(discount);
        try {
            connection.createStatement().execute("create index fkkjwchd8a4rwlxjafl4d152p41 on discount (invoice_item_id)");
            connection.createStatement().execute("alter table discount add constraint fkkjwchd8a4rwlxjafl4d152p41 " +
                                                 "foreign key (invoice_item_id) references invoice_item (id)");
        } catch (Exception e) {
        }

        // next the period reset table
        String periodReset = "create table if not exists period_reset " +
                             "( " +
                             " id varchar(255) not null " +
                             "  primary key, " +
                             " creation_timestamp timestamp(3) null, " +
                             " period_start timestamp(3) null, " +
                             " period_end timestamp(3) null, " +
                             " reason varchar(255) null, " +
                             " reset_by varchar(255) null, " +
                             " subscription_invoice_item_id varchar(255) null, " +
                             " applicable_financial_events varchar(255) null, " +
                             " constraint fkgbx59m8mrc4s4kd36ftpegikp " +
                             "  foreign key (subscription_invoice_item_id) references invoice_item (id) " +
                             ") ";
        connection.createStatement().execute(periodReset);
        try {
            connection.createStatement().execute("create index fkgbx59m8mrc4s4kd36ftpegikp on period_reset (subscription_invoice_item_id)");
        } catch (Exception e) {
        }
    }

    @Override
    public void prepareStatements() {
    }


    private PreparedStatement getInvoiceInsertPS(Connection connection) throws SQLException {
        String s = "insert into invoice (id, customer_id, transaction_type, status, transaction_processing_status, created_by, " +
                   "creation_timestamp, updated_by, update_timestamp, billing_entity, billing_zip, region, country, time_zone, " +
                   "legacy_order_id, payment_reference_id, source_invoice_id, source_legacy_order_id, version, sql_version, resubmit_timestamp)" +
                   "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        return connection.prepareStatement(s);
    }

    private PreparedStatement getFinancialEventInsertPS(Connection connection) throws SQLException {
        String s = "insert into financial_event (id, customer_id, financial_event_type, created_by, creation_timestamp, event_timestamp, " +
                   "billing_entity, amount_before_tax, tax, currency, card_type, last_four, mop_id, payment_processor, settlement_entity, " +
                   "invoice_id, vault_mop_sub_type, vault_mop_type, processing_method, item_to_amount) " +
                   "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        return connection.prepareStatement(s);
    }

    private PreparedStatement getFinancialEvenAttrInsertPS(Connection connection) throws SQLException {
        String s = "insert into financial_event_attribute (id, name, value) values (?,?,?)";
        return connection.prepareStatement(s);
    }

    private PreparedStatement getStatusChangeEventInsertPS(Connection connection) throws SQLException {
        String s = "insert into status_change_event (id, customer_id, created_by, creation_timestamp, event_timestamp, status_change_reason, invoice_id) " +
                   "values (?,?,?,?,?,?,?)";
        return connection.prepareStatement(s);
    }

    private PreparedStatement getInvoiceItemInsertPS(Connection connection) throws SQLException {
        String s = "insert into invoice_item (id, customer_id, item_type, created_by, creation_timestamp, updated_by, update_timestamp, " +
            "subscription_id, service_type, period_num, period_start, period_end, item_price, item_price_currency, selling_price_before_tax, " +
            "selling_price_tax, tax_percent, selling_price_currency, received_amount_before_tax, received_amount_tax, received_amount_currency, " +
            "invoice_id, subscription_start, source_invoice_item_id, issue_credit_type, plan_change_type, plan_change_id) " +
            "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        return connection.prepareStatement(s);
    }
    private PreparedStatement getInvoiceItemAttrInsertPS(Connection connection) throws SQLException {
        String s = "insert into invoice_item_attribute (id, name, value) values (?,?,?)";
        return connection.prepareStatement(s);
    }

    private PreparedStatement getPeriodResetInsertPS(Connection connection) throws SQLException
    {
        String s = "insert into period_reset (id, creation_timestamp, period_start, period_end, reason, reset_by, subscription_invoice_item_id, applicable_financial_events) " +
            "values (?,?,?,?,?,?,?,?)";
        return connection.prepareStatement(s);
    }

    private PreparedStatement getDiscountInsertPS(Connection connection) throws SQLException
    {
        String s = "insert into discount (id, discount_type, amount, currency, discount_id, invoice_item_id) value (?,?,?,?,?,?)";
        return connection.prepareStatement(s);
    }

    private PreparedStatement getBalanceConsumptionInsertPS(Connection connection) throws SQLException {
        String s = "insert into balance_consumption (id, balance_type, consumption_invoice_id, amount, currency, redemption_date, " +
                   "redemption_id, reference_id, invoice_item_id, created_by, creation_timestamp) " + "values (?,?,?,?,?,?,?,?,?,?,?)";
        return connection.prepareStatement(s);
    }
}
