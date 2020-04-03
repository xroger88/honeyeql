(ns honeyeql.db-adapter.mssql
  (:require [honeyeql.meta-data :as heql-md]
            [honeyeql.core :as heql]
            [honeysql.core :as hsql]
            [clojure.string :as string]))

(defmethod heql-md/get-db-config "Microsoft SQL Server" [_]
  {:schema             {:default "dbo"
                        :ignore  #{"information_schema" "sys" "guest" "db_accessadmin"
                                   "db_backoperator" "db_datareader" "db_datawriter"
                                   "db_ddladmin" "db_denydatareader" "db_denydatawriter"
                                   "db_owner" "db_securityadmin"}}
   :foreign-key-suffix "_id"})

(defn- mssql-type->col-type [{:keys [type_name]}]
  (case type_name
    ("CHAR" "NCHAR" "VARCHAR" "NVARCHAR" "TEXT" "NTEXT" "BINARY" "VARBINARY" "IMAGE") :attr.type/string
    ("TINYINT" "SMALLINT" "INT") :attr.type/integer
    "BIGINT" :attr.type/big-integer
    ("DECIMAL", "NUMERIC") :attr.type/decimal
    ("REAL" "FLOAT") :attr.type/float
    "XML" :attr.type/xml
    "DATE" :attr.type/date
    ("DATETIME" "DATETIME2" "SMALLDATETIME") :attr.type/data-time
    "DATETIMEOFFSET" :attr.type/offset-date-time
    "TIME" :attr.type/time
    :attr.type/unknown))

(defmethod heql-md/derive-attr-type "Microsoft SQL Server" [_ column-meta-data]
  (mssql-type->col-type column-meta-data))

(defn- entities-meta-data [db-spec jdbc-meta-data catalog]
  (->> (into-array String ["TABLE" "VIEW"])
       (.getTables jdbc-meta-data catalog "%" nil)
       (heql-md/datafied-result-set db-spec)
       vec))

(defn- attributes-meta-data [db-spec jdbc-meta-data catalog]
  (->> (.getColumns jdbc-meta-data catalog "%" "%" nil)
       (heql-md/datafied-result-set db-spec)
       vec))

(defn- primary-keys-meta-data [db-spec jdbc-meta-data catalog table-name]
  (->> (.getPrimaryKeys jdbc-meta-data catalog "" table-name)
       (heql-md/datafied-result-set db-spec)
       vec))

(defn- foreign-keys-meta-data [db-spec jdbc-meta-data catalog table-name]
  (->> (.getImportedKeys jdbc-meta-data catalog "" table-name)
       (heql-md/datafied-result-set db-spec)
       vec))

(defn- get-pks-and-fks [db-spec jdbc-meta-data catalog table-names]
  (reduce (fn [s table-name]
            (update (update s
                    :primary-keys (comp vec concat) (primary-keys-meta-data db-spec jdbc-meta-data catalog table-name))
                    :foreign-keys (comp vec concat) (foreign-keys-meta-data db-spec jdbc-meta-data catalog table-name)))
          {:primary-keys []
           :foreign-keys []} table-names))

(defmethod heql-md/get-db-meta-data "Microsoft SQL Server" [_ db-spec db-conn]
  (let [jdbc-meta-data                      (.getMetaData db-conn)
        catalog                             (.getCatalog db-conn)
        entities-meta-data                  (entities-meta-data db-spec jdbc-meta-data catalog)
        table-names                         (map :table_name entities-meta-data)
        {:keys [primary-keys foreign-keys]} (get-pks-and-fks db-spec jdbc-meta-data catalog table-names)]
    {:entities     entities-meta-data
     :attributes   (attributes-meta-data db-spec jdbc-meta-data catalog)
     :primary-keys primary-keys
     :foreign-keys foreign-keys}))

(defn- result-set-hql [hsql]
  (assoc hsql :for [(hsql/raw "JSON AUTO")]))

(defn- eql-node->select-expr [db-adapter heql-meta-data {:keys [attr-ident alias]
                                                         :as   eql-node}]
  (let [{:keys [parent self]} alias
        attr-md               (heql-md/attr-meta-data heql-meta-data attr-ident)
        select-attr-expr      (case (:attr.column.ref/type attr-md)
                                :attr.column.ref.type/one-to-one (keyword (str parent "__" self))
                                (:attr.column.ref.type/one-to-many :attr.column.ref.type/many-to-many) (heql/eql->hsql db-adapter heql-meta-data eql-node)
                                (->> (heql-md/attr-column-name attr-md)
                                     (str parent ".")
                                     keyword))]
    [select-attr-expr (heql/column-alias :qualified-kebab-case attr-ident)]))

(defn- assoc-one-to-one-hsql-queries [db-adapter heql-meta-data hsql eql-nodes]
  (->> (filter #(= :one-to-one-join (heql/find-join-type heql-meta-data %)) eql-nodes)
       (map (fn [{:keys [alias]
                  :as   eql-node}]
              [(hsql/raw "LATERAL")
               [(heql/eql->hsql db-adapter heql-meta-data eql-node)
                (keyword (str (:parent alias) "__" (:self alias)))]]))
       (update hsql :from #(apply concat %1 %2))))


(defrecord MssqlAdapter [db-spec heql-config heql-meta-data]
  heql/DbAdapter
  (db-spec [db-adapter]
    (:db-spec db-adapter))
  (meta-data [db-adapter]
    (:heql-meta-data db-adapter))
  (config [db-adapter]
    (:heql-config db-adapter))
  (to-sql [db-adapter hsql]
    (hsql/format (result-set-hql hsql) :quoting :sqlserver))
  (select-clause [db-adapter heql-meta-data eql-nodes]
    (vec (map #(eql-node->select-expr db-adapter heql-meta-data %) eql-nodes)))
  (resolve-children-one-to-one-relationships [db-adapter heql-meta-data hsql eql-nodes]
    (assoc-one-to-one-hsql-queries db-adapter heql-meta-data hsql eql-nodes))
  (resolve-one-to-one-relationship [db-adapter heql-meta-data hsql {:keys [children]}]
    (assoc-one-to-one-hsql-queries db-adapter heql-meta-data hsql children))
  (resolve-one-to-many-relationship [db-adapter heql-meta-data hsql {:keys [children]}]
    (assoc-one-to-one-hsql-queries db-adapter heql-meta-data hsql children))
  (resolve-many-to-many-relationship [db-adapter heql-meta-data hsql {:keys [children]}]
    (assoc-one-to-one-hsql-queries db-adapter heql-meta-data hsql children)))
