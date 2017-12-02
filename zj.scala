package com.finupgroup.alg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by finup on 2017/11/28.
  */
object ZJFeature {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("zjFeature")
      //      .config("spark.kryo.registrator", "com.finupgroup.alg.MyRegistrator")
      //      .config("conf spark.shuffle.manager", "hash")
      //      .config("spark.shuffle.consolidateFiles", true)
      .config("spark.reducer.maxSizeInFlight", "1024M")
      .config("spark.shuffle.io.preferDirectBufs", false)
      .getOrCreate()

    val start_date= args(0).toString
    val end_date= args(1).toString
    val out_path = args(2)

    import spark.implicits._

    //    val voice_info2 = spark.read.parquet("/user/hive/warehouse/alg_ods.db/kg_mobile_detail_voice_2_1")
    val voice_info = spark.read.parquet("/user/hive/warehouse/alg_ods_full_new_filter.db/kg_mobile_detail_voice_d")

    val ploan = spark.read.parquet("hdfs://192.168.176.62:8020/user/hive/warehouse/cif_jiea.db/p_loan")
    val ploan_m5 = ploan.filter($"create_time".gt(lit(start_date))).filter($"create_time".lt(lit(end_date))).select("apply_no", "create_time", "pass_time", "status", "is_over_due", "source", "user_id", "core_customer_id", "core_request_id")
    val ploan_m5_fk = ploan_m5.filter("status in (10,11,12)")

//    val lsDF = spark.read.parquet("hdfs://192.168.176.62:8020/user/hive/warehouse/cif.db/cif_policy_dw_loansummary_static").dropDuplicates().select("core_lend_request_id","due").toDF("ls_core_lend_request_id","ls_due")
    //val m5_fk = ploan_m5_fk.join(lsDF,$"core_request_id" === $"ls_core_lend_request_id", "left").drop("ls_core_lend_request_id")
    val odlsDF = spark.read.parquet("hdfs://192.168.176.62:8020/user/hive/warehouse/ods.db/ods_policy_dw_loansummary_static").dropDuplicates().select("core_lend_request_id","due").toDF("ls_core_lend_request_id","ls_due")
    val ploan_m5_fk_dueSta = ploan_m5_fk.join(odlsDF,$"core_request_id" === $"ls_core_lend_request_id", "left").drop("ls_core_lend_request_id")

    val user = spark.read.parquet("hdfs://192.168.176.62:8020/user/hive/warehouse/cif.db/cif_jiea_user")
    val user_phone = user.select("id", "id_no", "mobile")
    val m5_fk_phone = ploan_m5_fk_dueSta.join(user_phone, $"user_id" === $"id", "left").dropDuplicates()

    //    val m5_fk_phone = spark.read.parquet("hdfs://192.168.176.52:8020/user/cloud/neo4j/wxin/data/zj/qz_m5_fk_mobile")
// val nir_fk = spark.read.parquet("hdfs://192.168.176.52:8020/user/cloud/neo4j/wxin/data/zj/nirvana_fk_bef_m12_parq")
   val vi_dD = voice_info.dropDuplicates(Seq("id_no", "mobile", "counterpart_phone", "start_time")).filter($"start_time".gt(lit("2017-01-01")))
    val apply_opinfo = m5_fk_phone.toDF("p_apply_no", "p_create_time", "p_pass_time", "p_status", "p_is_over_due", "p_source", "p_user_id", "p_core_customer_id", "p_core_request_id", "p_ls_due", "p_id", "p_id_no", "p_mobile").join(vi_dD, $"p_id_no" === $"id_no", "left").filter("p_create_time > start_time")

//    apply_opinfo.write.parquet("hdfs://192.168.176.52:8020/user/cloud/neo4j/wxin/data/zj/apply_OpInfo_m5")

    val apply_tel_info = apply_opinfo.drop("business_name", "city", "communication_fee", "communication_loc", "id_no", "operator", "package_deals", "province", "src_create_time", "src_update_time", "create_time", "cif_dt", "communication_way", "communication_duration", "crawler_no", "id")

    val zjphone280 = spark.read.option("header", true).csv("hdfs://192.168.176.52:8020/user/cloud/neo4j/wxin/data/zj/zj_280w").dropDuplicates

    val apply_join_zj = zjphone280.join(apply_tel_info, $"agent_mobile" === $"counterpart_phone", "left")

    apply_join_zj.persist(StorageLevel.MEMORY_AND_DISK_SER)
    System.out.println("======" + apply_join_zj.count())

//    apply_join_zj.write.parquet("hdfs://192.168.176.52:8020/user/cloud/neo4j/wxin/data/zj/apply_join_zj280_m5")

    val double_pair = apply_join_zj.select("p_apply_no", "p_mobile", "counterpart_phone", "communication_way_code").dropDuplicates().groupBy("p_apply_no", "p_mobile", "counterpart_phone").count().filter("count > 1").drop("count")

    val applyTelInfoDouble = double_pair.join(apply_join_zj, Seq("p_apply_no", "p_mobile", "counterpart_phone"), "left")
    applyTelInfoDouble.persist(StorageLevel.MEMORY_AND_DISK_SER)
    System.out.println("======" + applyTelInfoDouble.count())

    val times = applyTelInfoDouble.select("p_apply_no", "p_mobile", "counterpart_phone").groupBy("p_apply_no", "p_mobile", "counterpart_phone").count()
    // times.write.parquet("hdfs://192.168.176.52:8020/user/cloud/neo4j/wxin/data/zj/apply_opInfo_times_m5")

    val inter = applyTelInfoDouble.select("p_apply_no", "p_create_time", "p_mobile", "counterpart_phone", "start_time").groupBy("p_apply_no", "p_create_time", "p_mobile", "counterpart_phone").agg(max($"start_time") as "last_time").withColumn("before_sec", $"p_create_time".cast("timestamp").cast("long") - $"last_time".cast("timestamp").cast("long"))
    // times.write.parquet("hdfs://192.168.176.52:8020/user/cloud/neo4j/wxin/data/zj/apply_opInfo_times_m5")

    val times_inter = inter.join(times, Seq("p_apply_no", "p_mobile", "counterpart_phone"), "left")

//    times_inter.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val applyOpInfoFeature = applyTelInfoDouble.join(times_inter, Seq("p_apply_no", "p_mobile", "counterpart_phone", "p_create_time"), "left").where("last_time = start_time").withColumnRenamed("count", "call_times").drop("isurge", "start_time", "counterpart_phone")


    applyOpInfoFeature.repartition(1).write.csv(out_path)

    spark.stop()

  }

}
