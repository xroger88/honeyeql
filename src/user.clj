(ns user
  (:require [next.jdbc.connection :as connection]
            [honeyeql.core :as heql]
            [honeyeql.db :as db]
            [hikari-cp.core :as hikari]
            [clojure.tools.namespace.repl :as tools-ns :refer [disable-reload! refresh clear set-refresh-dirs]]
            [taoensso.timbre :as log]))

(set-refresh-dirs "src")

(def db-adapter
  (db/initialize
   (hikari/make-datasource
    {:host "192.168.25.240"
     :port "41051"
     :maximum-pool-size 1
     :jdbc-url "jdbc:sqlserver://192.168.25.240:41051;DATABASENAME=USER"
     :driver-class-name "com.microsoft.sqlserver.jdbc.SQLServerDriver"
     :username "QAWeb_ACC"
     :password "wkf@ro$qkf5"})))

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

(comment

  (def tables (map #(key %) (:entities (heql/meta-data db-adapter))))

  (heql/query-single
   db-adapter
   [{'([] {:offset 2}) [:tb-player/player-id :tb-player/game-cd]}])

  (heql/query
   db-adapter
   [{[] [:tb-player/player-id :tb-player/game-cd]}])
)
