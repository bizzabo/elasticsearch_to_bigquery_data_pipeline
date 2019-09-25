package com.bizzabo.dataPipelineFromElasticsearchToBigquery

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import mu.KotlinLogging
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.values.TypeDescriptor
import org.joda.time.DateTimeZone
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.ISODateTimeFormat

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {

    val argMap = generateArgMap(args)
    val options = createPipelineOptions(argMap)
    FileSystems.setDefaultPipelineOptions(options)
    val pipeline = Pipeline.create(options)
    pipeline.coderRegistry.registerCoderForClass(TableRow::class.java, TableRowJsonCoder.of())

    val query: String = generateQuery(argMap)

    val tableReference = tableReference(argMap)

    val fields = setFields()

    pipeline.apply("Read from ES", readFromES(argMap, query))
            .setCoder(StringUtf8Coder.of())
            .apply("MapToTableRow", mapToTableRow())
            .setCoder(TableRowJsonCoder.of())
            .apply("Write to BQ", writeToBQ(tableReference, fields))
    pipeline.run()
}

private fun generateQuery(argMap: HashMap<String, String>): String {
    return when (argMap["queryType"]) {
        "daysAgo" -> {
            logger.info("Query will return documents modified between dates provided as days ago")
            queryModifiedBetweenDates(
                    getDaysAgoDateAsLong(argMap["daysBeforeStart"]!!.toInt()),
                    getDaysAgoDateAsLong(argMap["daysBeforeEnd"]!!.toInt()))
        }
        "betweenDates" -> {
            logger.info("Query will return documents modified between dates provided as dates")
            queryModifiedBetweenDates(
                    getDateAsLong(argMap["beginDate"]!!),
                    getDateAsLong(argMap["endDate"]!!))
        }
        "everything" -> {
            logger.info("Query will return all documents without constraints")
            queryAllBuilder()
        }
        "withSearchParam" -> {
            logger.info("Query will return documents which meet provided condition")
            queryBuilder(
                    argMap["paramName"]!!,
                    argMap["paramValue"]!!)
        }
        else -> {
            logger.info("Query type not provided or invalid. Query will return documents modified yesterday")
            queryModifiedBetweenDates(
                    getDaysAgoDateAsLong(1),
                    getDaysAgoDateAsLong(0))
        }
    }
}

private fun tableReference(argMap: HashMap<String, String>): TableReference? {
    return TableReference()
            .set("projectId", argMap["projectId"]!!)
            .set("datasetId", argMap["datasetId"]!!)
            .set("tableId", argMap["tableId"]!!)
}

private fun generateArgMap(args: Array<String>): HashMap<String, String> {
    val optionMap: MutableMap<String, String> = HashMap()
    val defaultArgMap: MutableMap<String, String> = HashMap()

    populateDefaultArgMap(defaultArgMap)

    for (arg in args) {
        optionMap[arg.substringBefore('=').substringAfter("--")] = arg.substringAfter('=')
    }

    val builder = StringBuilder()
    for (key in defaultArgMap.keys) {
        if (!optionMap.contains(key)) {
            builder.append("Mandatory option \"$key\" was not passed as argument. Default value: \"" + defaultArgMap[key] + "\" will be used instead.\n")
            optionMap[key] = defaultArgMap[key].toString()
        }
    }

    builder.append("Pipeline will be created with the following options:\n")
    for (key in optionMap.keys) {
        builder.append("\t$key: " + optionMap[key] + "\n")
    }
    logger.info(builder.toString())
    return optionMap as HashMap
}

private fun populateDefaultArgMap(defaultArgMap: MutableMap<String, String>) {
    defaultArgMap["batchSize"] = "5000"
    defaultArgMap["beginDate"] = "20190101"
    defaultArgMap["connectTimeout"] = "5000"
    defaultArgMap["datasetId"] = "datasetId"
    defaultArgMap["daysBeforeEnd"] = "0"
    defaultArgMap["daysBeforeStart"] = "1"
    defaultArgMap["diskSizeGb"] = "100"
    defaultArgMap["enableCloudDebugger"] = "true"
    defaultArgMap["endDate"] = "20190102"
    defaultArgMap["gcpTempLocation"] = "gs://dataPipelineFromElasticsearchToBigquery/gcpTempLocation/"
    defaultArgMap["index"] = "elasticsearchIndex"
    defaultArgMap["network"] = "gcp_network"
    defaultArgMap["numWorkers"] = "1000"
    defaultArgMap["paramName"] = "attributes.paramName.raw"
    defaultArgMap["paramValue"] = "Zohar"
    defaultArgMap["project"] = "gcpProject"
    defaultArgMap["projectId"] = "gcpProjectId"
    defaultArgMap["queryType"] = "yesterday"
    defaultArgMap["region"] = "gcpRegion"
    defaultArgMap["serviceAccount"] = "service@account.iam.gserviceaccount.com"
    defaultArgMap["socketAndRetryTimeout"] = "90000"
    defaultArgMap["source"] = "http://elasticsearch.data.source.com:9200"
    defaultArgMap["subnetwork"] = "regions/gcpRegion/subnetworks/subNetwork"
    defaultArgMap["tableId"] = "table"
    defaultArgMap["tempLocation"] = "gs://dataPipelineFromElasticsearchToBigquery/tempLocation/"
    defaultArgMap["type"] = "documentType"
    defaultArgMap["usePublicIps"] = "false"
}

private fun setFields(): java.util.ArrayList<TableFieldSchema> {
    return arrayListOf(
            TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED"),
            TableFieldSchema().setName("first_name").setType("STRING").setMode("REQUIRED"),
            TableFieldSchema().setName("last_name").setType("STRING").setMode("REQUIRED"),
            TableFieldSchema().setName("address").setType("STRING").setMode("REQUIRED"),
            TableFieldSchema().setName("birthday").setType("TIMESTAMP").setMode("REQUIRED"),
            TableFieldSchema().setName("person_json").setType("STRING").setMode("REQUIRED"),
            TableFieldSchema().setName("created").setType("TIMESTAMP").setMode("NULLABLE"),
            TableFieldSchema().setName("modified").setType("TIMESTAMP").setMode("NULLABLE")
    )
}

private fun mapToTableRow() = MapElements.into(TypeDescriptor.of(TableRow::class.java))
        .via(ContactStringToTableRow())

private fun writeToBQ(tableReference: TableReference?, fields: ArrayList<TableFieldSchema>): BigQueryIO.Write<TableRow>? {
    return BigQueryIO.writeTableRows()
            .to(tableReference)
            .optimizedWrites()
            .withSchema(TableSchema().setFields(fields))
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
}

private fun readFromES(argMap: HashMap<String, String>, query: String): ElasticsearchIO.Read? {
    return ElasticsearchIO.read()
            .withConnectionConfiguration(ElasticsearchIO.ConnectionConfiguration
                    .create(arrayOf(argMap["source"]), argMap["index"], argMap["type"])
                    .withConnectTimeout(argMap["connectTimeout"]!!.toInt())
                    .withSocketAndRetryTimeout(argMap["socketAndRetryTimeout"]!!.toInt()))
            .withBatchSize(argMap["batchSize"]!!.toLong())
            .withQuery(query)
}

private fun getDaysAgoDateAsLong(days: Int): Long {
    return LocalDate.now().minusDays(days).toDateTimeAtStartOfDay(DateTimeZone.UTC).millis
}

private fun getDateAsLong(date: String): Long {
    return DateTimeFormat.forPattern("YYYYMMdd").parseDateTime(date).millis
}

private fun createPipelineOptions(argMap: HashMap<String, String>): PipelineOptions {
    val options = PipelineOptionsFactory.`as`(DataflowPipelineOptions::class.java)
    options.project = argMap["project"]
    options.tempLocation = argMap["tempLocation"]
    options.gcpTempLocation = argMap["gcpTempLocation"]
    options.serviceAccount = argMap["serviceAccount"]
    options.region = argMap["region"]
    options.network = argMap["network"]
    options.subnetwork = argMap["subnetwork"]
    options.usePublicIps = argMap["usePublicIps"]!!.toBoolean()
    options.numWorkers = argMap["numWorkers"]!!.toInt()
    options.diskSizeGb = argMap["diskSizeGb"]!!.toInt()

    options.runner = DataflowRunner::class.java
    if (argMap["enableCloudDebugger"]!!.toBoolean()) {
        options.enableCloudDebugger
    }
    return options
}

private fun queryAllBuilder(): String {
    return """
            {
                "query": {
                    "match_all" : {}
                }
            }
        """.trimIndent()
}

private fun queryModifiedBetweenDates(beginDate: Long, endDate: Long): String {
    return """
        {
            "query": {
                "bool": {
                    "must": {
                        "match_all": {}
                    },
                    "filter": {
                        "bool": {
                            "must": {
                                "range": {
                                    "modified": {
                                        "from": $beginDate,
                                        "to": $endDate,
                                        "include_lower": true,
                                        "include_upper": false
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    """.trimIndent()
}

private fun queryBuilder(paramName: String, paramValue: String): String {
    return """
        {
            "query": {
                "bool": {
                    "must": [
                        { "match": { "$paramName": "$paramValue" }}
                    ]
                }
            }
        }
		""".trimIndent()
}

class ContactStringToTableRow : SimpleFunction<String, TableRow>() {
    override fun apply(input: String): TableRow {
        val gson: Gson = GsonBuilder().create()
        val parsedMap: Map<String, Any> = gson.fromJson(input, object : TypeToken<Map<String, Any>>() {}.type)
        return TableRow()
                .set("id", parsedMap["id"].toString().toDouble().toLong())
                .set("first_name", parsedMap["first_name"].toString())
                .set("last_name", parsedMap["last_name"].toString())
                .set("address", parsedMap["address"].toString())
                .set("birthday", ISODateTimeFormat.dateTime().print((parsedMap["birthday"].toString().toDouble().toLong())))
                .set("person_json", input)
                .set("created", ISODateTimeFormat.dateTime().print((parsedMap["created"].toString().toDouble().toLong())))
                .set("modified", ISODateTimeFormat.dateTime().print((parsedMap["modified"].toString().toDouble().toLong())))
    }
}
