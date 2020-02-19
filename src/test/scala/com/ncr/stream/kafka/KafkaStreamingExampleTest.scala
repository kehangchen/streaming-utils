package com.ncr.stream.kafka

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.TopologyTestDriver
import org.scalatest.funsuite.AnyFunSuite

class KafkaStreamingExampleTest extends AnyFunSuite {

  test("Test size is 2") {
    val ks = new KafkaStreamingExample("./application.conf")
    val driver = new TopologyTestDriver(ks.init(),
      ks.config.getProperties(ks.StreamingConfigPath))
    val input = ks.config.getString(ks.InputTopicPath)
    val output = ks.config.getString(ks.OutputTopicPath)
    val input_topic = driver.createInputTopic(input, new StringSerializer, new StringSerializer)
    val output_topic = driver.createOutputTopic(output, new StringDeserializer, new StringDeserializer)

    input_topic.pipeInput("{\"header\":{\"transaction_id\":\"20191124235959854\",\"source_system\":\"DCS\",\"tx_times\":\"2019-11-2423:59:59\",\"target_system\":\"EDL\",\"table_name\":\"DATA_TABLE\",\"instance_name\":\"3375\",\"batch_size\":\"500\"},\"Body\":[{\"entry_id\":2233668899},{\"entry_id\":1887928,\"entity_id\":\"TW.RSM.SFW.SoftwareManager.DeliveryTarget\",\"entity_key\":\"3c0b3d5f-062f-4d1a-8ebe-52f5162c35a7\",\"field_values\":{\"ID\":\"3c0b3d5f-062f-4d1a-8ebe-52f5162c35a7\",\"Name\":\"FFF_00_07_32_5B_F8_EB\",\"Status\":\"completed\",\"Completed\":true,\"CampaignID\":\"311dc434-b482-4cf1-be34-dbc6c3eaaa29\",\"Parameters\":{\"rows\":[],\"dataShape\":{\"fieldDefinitions\":{\"Name\":{\"name\":\"Name\",\"aspects\":{\"isPrimaryKey\":true},\"ordinal\":1,\"baseType\":\"STRING\",\"description\":\"\"},\"Value\":{\"name\":\"Value\",\"aspects\":{},\"ordinal\":2,\"baseType\":\"STRING\",\"description\":\"\"}}}},\"InstallDate\":1574658000000,\"DefinitionID\":\"8b4e1ca3-86b6-4cc5-94e5-fbbf0a55dff5\",\"DownloadDate\":1574446739768,\"StatusMessage\":\"\",\"StatusUpdated\":1574657981036,\"MaxNumAutoRetries\":0,\"AutoRetriesRemaining\":0},\"location\":\"0.0,0.0,0.0\",\"source_id\":\"TW.RSM.SFW.SoftwareManager\",\"source_type\":\"Thing\",\"tags\":[],\"time\":\"2019-11-24T23:59:41.039-05:00\",\"full_text\":\"completed{\\\"rows\\\":[],\\\"dataShape\\\":{\\\"fieldDefinitions\\\":{\\\"Value\\\":{\\\"name\\\":\\\"Value\\\",\\\"aspects\\\":{},\\\"description\\\":\\\"\\\",\\\"baseType\\\":\\\"STRING\\\",\\\"ordinal\\\":2},\\\"Name\\\":{\\\"name\\\":\\\"Name\\\",\\\"aspects\\\":{\\\"isPrimaryKey\\\":true},\\\"description\\\":\\\"\\\",\\\"baseType\\\":\\\"STRING\\\",\\\"ordinal\\\":1}}}}0.02019-11-22T13:18:59.768-05:002019-11-25T00:00:00.000-05:00FFF_00_07_32_5B_F8_EB8b4e1ca3-86b6-4cc5-94e5-fbbf0a55dff50.0311dc434-b482-4cf1-be34-dbc6c3eaaa29true3c0b3d5f-062f-4d1a-8ebe-52f5162c35a72019-11-24T23:59:41.036-05:00\"}]}")
    assert(output_topic.readKeyValuesToList().size() === 2)
  }
}
