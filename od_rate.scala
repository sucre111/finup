import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

val m5 = spark.read.csv("hdfs://192.168.176.52:8020/user/cloud/neo4j/wxin/data/zj/applyOpInfoFeature_m5_dueSta").toDF("p_apply_no", "p_mobile", "p_create_time", "agent_mobile", "p_pass_time", "p_status", "p_is_over_due", "p_source", "p_user_id", "p_core_customer_id", "p_core_request_id", "p_id", "p_id_no", "communication_type", "communication_way_code", "mobile", "duration_second", "source_no", "last_time", "before_sec", "call_times", "due")

val basic_m5 = m5.select("p_apply_no", "p_id_no", "p_mobile", "p_create_time", "agent_mobile")
// val bef_month = "2017-06-01"
val voice_info = spark.read.parquet("/user/hive/warehouse/alg_ods_full_new_filter.db/kg_mobile_detail_voice_d").dropDuplicates(Seq("id_no", "mobile", "counterpart_phone", "start_time"))

val agent_join_user = basic_m5.join(voice_info, $"agent_mobile"===$"counterpart_phone","left").filter("p_id_no != id_no and p_create_time > start_time")
agent_join_user.persist(StorageLevel.MEMORY_AND_DISK_SER)

val agent_user_double_pair = agent_join_user.select("p_apply_no", "agent_mobile", "id_no", "communication_way_code").dropDuplicates().groupBy("p_apply_no", "agent_mobile", "id_no").count().filter("count > 1").drop("count")

val apply_agent_dtUser = agent_user_double_pair.join(agent_join_user, Seq("p_apply_no", "agent_mobile", "id_no"), "left")

val agent_dtUser = apply_agent_dtUser.select("p_apply_no", "p_create_time", "agent_mobile", "id_no")

val kg_apply_basic_info = spark.read.parquet("/user/hive/warehouse/alg_ods_full_new_filter.db/kg_apply_basic_info")

val dtUser_apply = agent_dtUser
