(ns user
  (:require [next.jdbc.connection :as connection]
            [honeyeql.core :as heql]
            [honeyeql.meta-data :as heql-md]
            [honeyeql.db :as db]
            [next.jdbc :as jdbc]
            [hikari-cp.core :as hikari]
            [clojure.tools.namespace.repl :as tools-ns :refer [disable-reload! refresh clear set-refresh-dirs]]
            [taoensso.timbre :as log]))

(set-refresh-dirs "src")

;; log config
(def logging-config {:level        :info
                 :ns-whitelist []
                 :ns-blacklist ["com.mchange.v2.c3p0.impl.C3P0PooledConnectionPool"
                                "com.mchange.v2.c3p0.stmt.GooGooStatementCache"
                                "com.mchange.v2.resourcepool.BasicResourcePool"
                                "com.zaxxer.hikari.pool.HikariPool"
                                "com.zaxxer.hikari.pool.PoolBase"
                                "com.mchange.v2.c3p0.impl.AbstractPoolBackedDataSource"
                                "com.mchange.v2.c3p0.impl.NewPooledConnection"
                                "datomic.common"
                                "datomic.connector"
                                "datomic.coordination"
                                "datomic.db"
                                "datomic.index"
                                "datomic.kv-cluster"
                                "datomic.log"
                                "datomic.peer"
                                "datomic.process-monitor"
                                "datomic.reconnector2"
                                "datomic.slf4j"
                                "io.netty.buffer.PoolThreadCache"
                                "org.apache.http.impl.conn.PoolingHttpClientConnectionManager"
                                "org.projectodd.wunderboss.web.Web"
                                "org.quartz.core.JobRunShell"
                                "org.quartz.core.QuartzScheduler"
                                "org.quartz.core.QuartzSchedulerThread"
                                "org.quartz.impl.StdSchedulerFactory"
                                "org.quartz.impl.jdbcjobstore.JobStoreTX"
                                "org.quartz.impl.jdbcjobstore.SimpleSemaphore"
                                "org.quartz.impl.jdbcjobstore.StdRowLockSemaphore"
                                "org.quartz.plugins.history.LoggingJobHistoryPlugin"
                                "org.quartz.plugins.history.LoggingTriggerHistoryPlugin"
                                "org.quartz.utils.UpdateChecker"
                                "shadow.cljs.devtools.server.worker.impl"]})
;; set config
(log/merge-config! logging-config)

(def cpdev-db-spec
    {:host "192.168.25.240"
     :port "41051"
     :maximum-pool-size 1
     :jdbc-url "jdbc:sqlserver://192.168.25.240:41051;DATABASENAME=USER"
     :driver-class-name "com.microsoft.sqlserver.jdbc.SQLServerDriver"
     :username "QAWeb_ACC"
     :password "wkf@ro$qkf5"})

(def mypc-db-spec
    {:host "172.27.1.65"
     :port "41051"
     :maximum-pool-size 1
     :jdbc-url "jdbc:sqlserver://172.27.1.65:41051"
     :driver-class-name "com.microsoft.sqlserver.jdbc.SQLServerDriver"
     :username "sa"
     :password "@Kolee89*"})

(def db-adapter
  (db/initialize (hikari/make-datasource mypc-db-spec)))

(defn start []
  :ok)

(defn stop []
  :ok)

(def go start)

(defn restart
  "Stop, refresh, and restart the server."
  []
  (stop)
  (tools-ns/refresh :after 'user/start))

(def reset #'restart)


(defn get-mydb-meta-data [db-spec catalog schema table column]
  (with-open [db-source (hikari/make-datasource db-spec)
              db-conn (jdbc/get-connection db-source)]
    (let [jdbc-meta-data (.getMetaData db-conn)
          current-catalog (.getCatalog db-conn)
          selected-catalog (if (not= current-catalog catalog)
                             (do
                               (.setCatalog db-conn catalog) ;; chnage to calalog
                               catalog)
                             current-catalog)]
      {:tables (->> (into-array String ["TABLE" "VIEW"])
                    (.getTables jdbc-meta-data selected-catalog schema table)
                    (heql-md/datafied-result-set db-spec)
                    vec)
       :columns (->> (.getColumns jdbc-meta-data selected-catalog schema table column)
                    (heql-md/datafied-result-set db-spec)
                    vec)})))

(comment

  (def mypc-db-spec (assoc mypc-db-spec :jdbc-url "jdbc:sqlserver://172.27.1.65:41051"))
  (def db-spec (hikari/make-datasource mypc-db-spec)) ;; datasource
  (def db-conn (jdbc/get-connection db-spec)) ;; get connection from hikari pool
  (def jdbc-meta-data  (.getMetaData db-conn)) ;; get meta-data from connection
  (def catalog    (.getCatalog db-conn)) ;; default is master database if database name is not specified in jdbc-url
  (.setCatalog db-conn "BikeStores") ;; change to BikeStores database or any database available in connection
  (def tables  (->> (into-array String ["TABLE" "VIEW"])
                    (.getTables jdbc-meta-data (.getCatalog db-conn) "%" nil)
                    (heql-md/datafied-result-set db-spec)
                    vec))
  (def columns (->> (.getColumns jdbc-meta-data (.getCatalog db-conn) "%" "%" nil)
                    (heql-md/datafied-result-set db-spec)
                    vec))

  ;; how to filer out tables and columns only interesting
  (def filter-config {:database ["BikeStores"]
                      :schema ["production" "sales"]
                      :table ["customers" "brands"]})

  (def brands (get-mydb-meta-data mypc-db-spec "BikeStores" "production" "brands" nil))
  (def customers (get-mydb-meta-data mypc-db-spec "BikeStores" "sales" "customers" nil))


  (def heql-tables (map #(key %) (:entities (heql/meta-data db-adapter))))

  (heql/query
   db-adapter
   [{[] [:sales.customer/customer-id :sales.customer/first-name]}])

  (heql/query-single
   db-adapter
   [{'([] {:offset 2}) [:sales.customer/customer-id :sales.customer/first-name]}])

)
