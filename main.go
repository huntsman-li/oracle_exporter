package main

import (
	"database/sql"
	"flag"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-oci8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"fmt"
)

var (
	// Version will be set at build time.
	Version       = "0.0.0.dev"
	listenAddress = flag.String("web.listen-address", ":9161", "Address to listen on for web interface and telemetry.")
	metricPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	landingPage   = []byte("<html><head><title>Oracle DB exporter</title></head><body><h1>Oracle DB exporter</h1><p><a href='" + *metricPath + "'>Metrics</a></p></body></html>")
)

// Metric name parts.
const (
	namespace = "oracledb"
	exporter  = "exporter"
)

// Exporter collects Oracle DB metrics. It implements prometheus.Collector.
type Exporter struct {
	dsn             string
	duration, error prometheus.Gauge
	totalScrapes    prometheus.Counter
	scrapeErrors    *prometheus.CounterVec
	up              prometheus.Gauge
}

// NewExporter returns a new Oracle DB exporter for the provided DSN.
func NewExporter(dsn string) *Exporter {
	return &Exporter{
		dsn: dsn,
		duration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_duration_seconds",
			Help:      "Duration of the last scrape of metrics from Oracle DB.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrapes_total",
			Help:      "Total number of times Oracle DB was scraped for metrics.",
		}),
		scrapeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrape_errors_total",
			Help:      "Total number of times an error occured scraping a Oracle database.",
		}, []string{"collector"}),
		error: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from Oracle DB resulted in an error (1 for error, 0 for success).",
		}),
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether the Oracle database server is up.",
		}),
	}
}

// Describe describes all the metrics exported by the MS SQL exporter.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics. The problem
	// here is that we need to connect to the Oracle DB. If it is currently
	// unavailable, the descriptors will be incomplete. Since this is a
	// stand-alone exporter and not used as a library within other code
	// implementing additional metrics, the worst that can happen is that we
	// don't detect inconsistent metrics created by this exporter
	// itself. Also, a change in the monitored Oracle instance may change the
	// exported metrics during the runtime of the exporter.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh

}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.scrape(ch)
	ch <- e.duration
	ch <- e.totalScrapes
	ch <- e.error
	e.scrapeErrors.Collect(ch)
	ch <- e.up
}

func (e *Exporter) scrape(ch chan<- prometheus.Metric) {
	e.totalScrapes.Inc()
	var err error
	defer func(begun time.Time) {
		e.duration.Set(time.Since(begun).Seconds())
		if err == nil {
			e.error.Set(0)
		} else {
			e.error.Set(1)
		}
	}(time.Now())

	db, err := sql.Open("oci8", e.dsn)
	if err != nil {
		log.Errorln("Error opening connection to database:", err)
		return
	}
	defer db.Close()

	isUpRows, err := db.Query("SELECT 1 FROM DUAL")
	if err != nil {
		log.Errorln("Error pinging oracle:", err)
		e.up.Set(0)
		return
	}
	isUpRows.Close()
	e.up.Set(1)

	if err = ScrapeActivity(db, ch); err != nil {
		log.Errorln("Error scraping for activity:", err)
		e.scrapeErrors.WithLabelValues("activity").Inc()
	}

	if err = ScrapeWaitTime(db, ch); err != nil {
		log.Errorln("Error scraping for wait_time:", err)
		e.scrapeErrors.WithLabelValues("wait_time").Inc()
	}

	if err = ScrapeSessions(db, ch); err != nil {
		log.Errorln("Error scraping for sessions:", err)
		e.scrapeErrors.WithLabelValues("sessions").Inc()
	}
	if err = ScrapeSessionNum(db, ch); err != nil {
		log.Errorln("Error scraping for SessionNum:", err)
		e.scrapeErrors.WithLabelValues("SessionNum").Inc()
	}
	if err = ScrapeFsfi(db, ch); err != nil {
		log.Errorln("Error scraping for Fsfi:", err)
		e.scrapeErrors.WithLabelValues("Fsfi").Inc()
	}
	//if err = ScrapeTableSapce(db, ch); err != nil {
	//	log.Errorln("Error scraping for TableSapce:", err)
	//	e.scrapeErrors.WithLabelValues("TableSapce").Inc()
	//}
	if err = ScrapePct(db, ch); err != nil {
		log.Errorln("Error scraping for Pct:", err)
		e.scrapeErrors.WithLabelValues("Pct").Inc()
	}
	if err = ScrapeDbVersion(db, ch); err != nil {
		log.Errorln("Error scraping for DbVersion:", err)
		e.scrapeErrors.WithLabelValues("DbVersion").Inc()
	}
	if err = ScrapeEventCount(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeEventCount:", err)
		e.scrapeErrors.WithLabelValues("ScrapeEventCount").Inc()
	}
	if err = ScrapeTableIo(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeTableIo:", err)
		e.scrapeErrors.WithLabelValues("ScrapeTableIo").Inc()
	}
	if err = ScrapeHitRadio(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeHitRadio:", err)
		e.scrapeErrors.WithLabelValues("ScrapeHitRadio").Inc()
	}
	if err = ScrapePinReload(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapePinReload:", err)
		e.scrapeErrors.WithLabelValues("ScrapePinReload").Inc()
	}
	if err = ScrapeRedoEntries(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeRedoEntries:", err)
		e.scrapeErrors.WithLabelValues("ScrapeRedoEntries").Inc()
	}
	if err = ScrapeSort(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeSort:", err)
		e.scrapeErrors.WithLabelValues("ScrapeSort").Inc()
	}
	if err = ScrapeRedoBufferAllocationRetries(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeRedoBufferAllocationRetries:", err)
		e.scrapeErrors.WithLabelValues("ScrapeRedoBufferAllocationRetries").Inc()
	}
	if err = ScrapeLogBufferRatio(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeLogBufferRatio:", err)
		e.scrapeErrors.WithLabelValues("ScrapeLogBufferRatio").Inc()
	}
	if err = ScrapeGetMisses(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeGetMisses:", err)
		e.scrapeErrors.WithLabelValues("ScrapeGetMisses").Inc()
	}
	if err = ScrapeUserConnectionsNum(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeUserConnectionsNum:", err)
		e.scrapeErrors.WithLabelValues("ScrapeUserConnectionsNum").Inc()
	}
	if err = ScrapeUserConnStatus(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeUserConnStatus:", err)
		e.scrapeErrors.WithLabelValues("ScrapeUserConnStatus").Inc()
	}
	if err = ScrapeRowCache(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeRowCache:", err)
		e.scrapeErrors.WithLabelValues("ScrapeRowCache").Inc()
	}
	if err = ScrapeTableSpace(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeTableSpace:", err)
		e.scrapeErrors.WithLabelValues("ScrapeTableSpace").Inc()
	}
	if err = ScrapeTempTableSpace(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeTempTableSpace:", err)
		e.scrapeErrors.WithLabelValues("ScrapeTempTableSpace").Inc()
	}
	if err = ScrapeSqlIoTopSpace(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeSqlIoTopSpace:", err)
		e.scrapeErrors.WithLabelValues("ScrapeSqlIoTopSpace").Inc()
	}

	if err = ScrapeRedoLogSwitchSpace(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeRedoLogSwitchSpace:", err)
		e.scrapeErrors.WithLabelValues("ScrapeRedoLogSwitchSpace").Inc()
	}

	if err = ScrapeMenDiskSortRatioSpace(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeMenDiskSortRatioSpace:", err)
		e.scrapeErrors.WithLabelValues("ScrapeMenDiskSortRatioSpace").Inc()
	}

	//if err = ScrapeSqlWaitingNum(db, ch); err != nil {
	//	log.Errorln("Error scraping for ScrapeSqlWaitingNum:", err)
	//	e.scrapeErrors.WithLabelValues("ScrapeSqlWaitingNum").Inc()
	//}

	if err = ScrapeSGAMemHit(db, ch); err != nil {
		log.Errorln("Error scraping for ScrapeSGAHit:", err)
		e.scrapeErrors.WithLabelValues("ScrapeSGAHit").Inc()
	}

	if err = ScrapeSGAShareMemHit(db, ch); err != nil {
		log.Errorln("Error scraping for SGAShareMemHit:", err)
		e.scrapeErrors.WithLabelValues("SGAShareMemHit").Inc()
	}

	if err = ScrapeSGARedoLogHit(db, ch); err != nil {
		log.Errorln("Error scraping for SGARedoLogHit:", err)
		e.scrapeErrors.WithLabelValues("SGARedoLogHit").Inc()
	}

}

//select sum(gets-getmisses-usage-fixed)/sum(gets) from v$rowcache
//select name,waits,gets,round(waits/gets*100,2)vvv from v$rollstat a,v$rollname b where a.USN=b.usn;
//select df.tablespace_name name,df.file_name "file",f.phyrds pyr,f.phyblkrd pbr,f.phywrts pyw,
//f.phyblkwrt pbw
//from v$filestat f,dba_data_files df
//where f.file#=df.file_id
//SELECT name profile,cnt,decode( total, 0, 0, round( cnt * 100 / total)) percentage FROM ( SELECT name, value cnt, ( SUM( value ) OVER()) total FROM v$sysstat WHERE name LIKE 'workarea exec%');
//SELECT SUM (pinhits) / SUM (pins) * 100 "hit radio" FROM v$librarycache;
//select sum(pins-reloads)/sum(pins) from v$librarycache
//SELECT a.VALUE redo_entries, b.VALUE redo_buffer_allocation_retries,ROUND ((1 - b.VALUE / a.VALUE) * 100, 4) log_buffer_ratio FROM v$sysstat a, v$sysstat b WHERE a.NAME = 'redo entries' AND b.NAME = 'redo buffer allocation retries';

func ScrapeSGARedoLogHit(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query(`SELECT name, gets, misses, immediate_gets, immediate_misses, Decode(gets,0,0,misses/gets*100) ratio1, Decode(immediate_gets+immediate_misses,0,0, immediate_misses/(immediate_gets+immediate_misses)*100) ratio2 FROM v$latch WHERE name IN ('redo allocation', 'redo copy')`)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var name, gets, misses, immediate_gets, immediate_misses, ratiO1 sql.NullString
		var rati02 float64
		if err := rows.Scan(&name, &gets, &misses, &immediate_gets, &immediate_misses, &ratiO1, &rati02); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "SGARedoLogHit", "info"), "Oracle SGARedoLogHit.",
				[]string{"Name", "Gets", "Misses", "ImmediateGets", "ImmediateMisses", "ratiO1"}, nil),
			prometheus.GaugeValue, rati02,
			name.String, gets.String, misses.String, immediate_gets.String, immediate_misses.String, ratiO1.String,
		)
	}
	return nil
}

func ScrapeSGAShareMemHit(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query(`select sum(pins-reloads)/sum(pins) *100 libcache from gv$librarycache`)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var name sql.NullString
		var Hit float64
		cleanName(name.String)
		name.String = "SGAShareMemHit"
		if err := rows.Scan(&Hit); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "SGAShareMemHit", "info"), "Oracle SGAShareMemHit.",
				[]string{"Name"}, nil),
			prometheus.GaugeValue, Hit,
			name.String,
		)
	}
	return nil
}

func ScrapeSGAMemHit(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query(`select (1-(sum(decode(name, 'physical reads',value,0))/(sum(decode(name, 'db block gets',value,0))  +sum(decode(name,'consistent gets',value,0))))) * 100 "Hit Ratio"  from gv$sysstat`)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var name sql.NullString
		var Hit float64
		cleanName(name.String)
		name.String = "SGAHitMem"
		if err := rows.Scan(&Hit); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "SGAMemHit", "info"), "Oracle SGAMemHit.",
				[]string{"Name"}, nil),
			prometheus.GaugeValue, Hit,
			name.String,
		)
	}
	return nil
}

func ScrapeTableSpace(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query(`SELECT A.TABLESPACE_NAME "TableSpaceName",A.TOTAL_SPACE "TotalSpace(MB)", NVL(B.FREE_SPACE, 0) "FreeSpace(MB)", A.TOTAL_SPACE - NVL(B.FREE_SPACE, 0) "UsedSpace(MB)", CASE WHEN A.TOTAL_SPACE=0 THEN 0 ELSE trunc(NVL(B.FREE_SPACE, 0) / A.TOTAL_SPACE * 100, 2) END "SpaceFreePercentage(%)" FROM (SELECT TABLESPACE_NAME, trunc(SUM(BYTES) / 1024 / 1024 ,2) TOTAL_SPACE FROM DBA_DATA_FILES GROUP BY TABLESPACE_NAME) A, (SELECT TABLESPACE_NAME, trunc(SUM(BYTES / 1024 / 1024  ),2) FREE_SPACE FROM DBA_FREE_SPACE GROUP BY TABLESPACE_NAME) B WHERE A.TABLESPACE_NAME = B.TABLESPACE_NAME(+) ORDER BY 5`)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var TableSpaceName, TotalSpace, FreeSpacesql, UsedSpace, SpaceFreePercentage sql.NullString
		var FreePercentage float64
		cleanName(SpaceFreePercentage.String)
		if err := rows.Scan(&TableSpaceName, &TotalSpace, &FreeSpacesql, &UsedSpace, &FreePercentage); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "TableSpace", "info"), "Oracle TableSpace.",
				[]string{"TableSpaceName", "TotalSpace_MB", "FreeSpace_MB", "UsedSpace_MB"}, nil),
			prometheus.GaugeValue, FreePercentage,
			TableSpaceName.String, TotalSpace.String, FreeSpacesql.String, UsedSpace.String,
		)
	}
	return nil
}

func ScrapeTempTableSpace(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query(`Select f.tablespace_name,sum(f.bytes_free + f.bytes_used) /1024/1024 "total MB",sum((f.bytes_free + f.bytes_used) - nvl(p.bytes_used, 0)) /1024/1024 "Free MB" ,sum(nvl(p.bytes_used, 0)) /1024/1024 "Used MB" from sys.v_$temp_space_header f, dba_temp_files d, sys.v_$temp_extent_pool p where f.tablespace_name(+) = d.tablespace_name and f.file_id(+) = d.file_id and p.file_id(+) = d.file_id group by f.tablespace_name`)

	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var TempTableName, total, free sql.NullString
		var UsedPercentage float64

		if err := rows.Scan(&TempTableName, &total, &free, &UsedPercentage); err != nil {
			return err
		}

		fmt.Println(total, free, UsedPercentage)

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "TempTableSpace", "info"), "Oracle TempTableSpace.",
				[]string{"TABLESPACE_NAME", "total_MB", "free_MB"}, nil),
			prometheus.GaugeValue, UsedPercentage,
			TempTableName.String, total.String, free.String,
		)
	}
	return nil
}

func ScrapeSqlIoTopSpace(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query(`select SQL_ID, CHILD_NUMBER, SQL_TEXT, ELAPSED_TIME, CPU_TIME, DISK_READS, ELAPSED_RANK
	from (select v.sql_id,
		v.child_number,
		v.sql_text,
		v.elapsed_time,
		v.cpu_time,
		v.disk_reads,
		rank() over(order by v.disk_reads desc) elapsed_rank
		from v$sql v) a
		where elapsed_rank <= 10`)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var (
			sql_id, sql_text, elapsed_time, cpu_time, disk_reads, rank sql.NullString
			child_number float64
		)

		if err := rows.Scan(&sql_id, &child_number, &sql_text, &elapsed_time, &cpu_time, &disk_reads, &rank); err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "SqlIoTopSpace", "info"), "Oracle SqlIoTopSpace.",
				[]string{"SQL_ID", "SQL_TEXT", "ELAPSED_TIME", "CPU_TIME", "DISK_READS", "ELAPSED_RANK"}, nil),
			prometheus.GaugeValue, child_number,
			sql_id.String, sql_text.String, elapsed_time.String, cpu_time.String, disk_reads.String, rank.String,
		)
	}
	return nil
}

func ScrapeRedoLogSwitchSpace(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query(`select b.SEQUENCE#, b.FIRST_TIME, a.SEQUENCE#, a.FIRST_TIME, round(((a.FIRST_TIME - b.FIRST_TIME) * 24) * 60, 2) diff from v$log_history a, v$log_history b where a.SEQUENCE# = b.SEQUENCE# + 1 and b.THREAD# = 1order by a.SEQUENCE# desc`)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var BeforeSequence, BeforeTime, AfterSequence, AfterTime sql.NullString
		var diff float64
		if err := rows.Scan(&BeforeSequence, &BeforeTime, &AfterSequence, &AfterTime, &diff); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "RedoLogSwitchSpace", "info"), "Oracle MenDiskSortRatioSpace.",
				[]string{"BeforeSequence", "BeforeTime", "AfterSequence", "AfterTime"}, nil),
			prometheus.GaugeValue, diff,
			BeforeSequence.String, BeforeTime.String, AfterSequence.String, AfterTime.String,
		)
	}
	return nil
}

func ScrapeSqlWaitingNum(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query(`select hash_value,child_number,
       lpad('',2*depth)
       ||operation
       ||''
       ||options
       ||decode(id,0,substr(optimizer,1,6)||'Cost='||to_char(cost))operation,
       object_name,object_type,cost,round(bytes/1024) kbytes
from v$sql_plan where hash_value in(
                     select a.sql_hash_value
                     from v$session a,v$session_wait b
                     where a.sid=b.sid
                     and b.event='db file sequential read')
order by hash_value,child_number,ID`)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var (
			hashValue, childNumber, objectName, objectType, cost sql.NullString
			kbytes float64
		)

		if err := rows.Scan(&hashValue, &childNumber, &objectName, &objectType, &cost); err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "SqlWaitingNum", "info"), "Oracle SqlWaitingNum.",
				[]string{"hashValue", "childNumber", "objectName", "objectType", "cost"}, nil),
			prometheus.GaugeValue, kbytes,
			hashValue.String, childNumber.String, objectName.String, objectType.String, cost.String,
		)
	}
	return nil
}

func ScrapeMenDiskSortRatioSpace(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query(`SELECT name, value/1024/1024 FROM v$sysstat WHERE name IN ('sorts (memory)', 'sorts (disk)')`)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var (
			name sql.NullString
			size float64
		)

		if err := rows.Scan(&name, &size); err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "MenDiskSortRatioSpace", "info"), "Oracle MenDiskSortRatioSpace.",
				[]string{"name"}, nil),
			prometheus.GaugeValue, size,
			name.String,
		)
	}
	return nil
}

func ScrapeGetMisses(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select sum(gets-getmisses-usage-fixed)/sum(gets) from v$rowcache")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var ReodBuffer string = "ScrapeGetMisses"
		var redo_entries float64
		if err := rows.Scan(&redo_entries); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "ScrapeGetMisses", "info"), "Oracle ScrapeGetMisses.",
				[]string{"ScrapeGetMisses_info"}, nil),
			prometheus.GaugeValue, redo_entries,
			ReodBuffer,
		)
	}
	return nil
}
func ScrapeRedoEntries(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT a.VALUE redo_entries FROM v$sysstat a, v$sysstat b WHERE a.NAME = 'redo entries' AND b.NAME = 'redo buffer allocation retries'")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var ReodBuffer string = "redo_entries"
		var redo_entries float64
		if err := rows.Scan(&redo_entries); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "redo_entries", "info"), "Oracle redo_entries.",
				[]string{"redo_entries_info"}, nil),
			prometheus.GaugeValue, redo_entries,
			ReodBuffer,
		)
	}
	return nil
}

func ScrapeLogBufferRatio(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT ROUND ((1 - b.VALUE / a.VALUE) * 100, 4) log_buffer_ratio FROM  v$sysstat a, v$sysstat b WHERE a.NAME = 'redo entries' AND b.NAME = 'redo buffer allocation retries'")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var LogBufferRatio string = "redo_entries"
		var value float64
		if err := rows.Scan(&value); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "ScrapeLogBufferRatio", "info"), "Oracle ScrapeLogBufferRatio.",
				[]string{"ScrapeLogBufferRatio"}, nil),
			prometheus.GaugeValue, value,
			LogBufferRatio,
		)
	}
	return nil
}
func ScrapeRedoBufferAllocationRetries(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT b.VALUE redo_buffer_allocation_retries FROM v$sysstat a, v$sysstat b WHERE a.NAME = 'redo entries' AND b.NAME = 'redo buffer allocation retries'")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var RedoBufferAllocationRetries string = "RedoBufferAllocationRetries"
		var value float64
		if err := rows.Scan(&value); err != nil {
			return err
		}
		log.Info("RedoBufferAllocationRetries", value)

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "ScrapeRedoBufferAllocationRetries", "info"), "Oracle ScrapeRedoBufferAllocationRetries.",
				[]string{"ScrapeRedoBufferAllocationRetries"}, nil),
			prometheus.GaugeValue, value,
			RedoBufferAllocationRetries,
		)
	}
	return nil
}
func ScrapeSort(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select name,value from v$sysstat where name like '%sort%'")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		//		var scrapeSort string
		var name string
		var value float64
		if err := rows.Scan(&name, &value); err != nil {
			return err
		}
		//		value_str :=strconv.Itoa(value)
		//		scrapeSort=name+":"+value_str
		//		log.Info("name:",scrapeSort)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "ScrapeSort", "info"), "Oracle ScrapeSort.",
				[]string{"ScrapeSort_info"}, nil),
			prometheus.GaugeValue, value,
			name,
		)
	}
	return nil
}
func ScrapePinReload(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select sum(pins-reloads)/sum(pins) from v$librarycache")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var pinReload string = "ScrapePinReload"
		var count float64
		//		var size_str string
		if err := rows.Scan(&count); err != nil {
			return err
		}
		//		count_str := strconv.FormatFloat(count, 'f', -1, 64)
		//		pinReload="ScrapePinReload:"+count_str
		//		log.Info("name:",pinReload)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "ScrapePinReload", "info"), "Oracle ScrapePinReload.",
				[]string{"ScrapePinReload_info"}, nil),
			prometheus.GaugeValue, count,
			pinReload,
		)
	}
	return nil
}
func ScrapeHitRadio(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT SUM (pinhits) / SUM (pins) * 100 FROM v$librarycache")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var radioName string = "hit radio"
		var count float64
		//		var size_str string
		if err := rows.Scan(&count); err != nil {
			return err
		}
		//		count_str := strconv.FormatFloat(count, 'f', -1, 64)
		//		radioName="hit radio :"+count_str
		//		log.Info("name:",radioName)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "ScrapeHitRadio", "info"), "Oracle ScrapeHitRadio.",
				[]string{"ScrapeHitRadio_info"}, nil),
			prometheus.GaugeValue, count,
			radioName,
		)
	}
	return nil
}

func ScrapeTableIo(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select df.tablespace_name name,f.phyrds pyr,f.phyblkrd pbr,f.phywrts pyw,f.phyblkwrt pbw from v$filestat f,dba_data_files df where f.file#=df.file_id")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		var pry int
		var pbr int
		var pyw int
		var pbw int
		//		var size_str string
		if err := rows.Scan(&name, &pry, &pbr, &pyw, &pbw); err != nil {
			return err
		}
		name = cleanName(name)
		pry_str := strconv.Itoa(pry)
		pbr_str := strconv.Itoa(pbr)
		pyw_str := strconv.Itoa(pyw)
		pbw_str := strconv.Itoa(pbw)
		name = name + ":" + "PYR:" + pry_str + "PBR:" + pbr_str + "PYW:" + pyw_str + "PBW:" + pbw_str
		log.Info("name:", name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "ScrapeTableIo", "info"), "Oracle ScrapeTableIo.",
				[]string{"ScrapeTableIo_info"}, nil),
			prometheus.GaugeValue, 1,
			name,
		)
	}
	return nil
}
func ScrapeEventCount(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select event, count(*)from v$session_wait group by event order by count(*) desc")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		var count float64
		//		var size_str string
		if err := rows.Scan(&name, &count); err != nil {
			return err
		}
		name = cleanName(name)
		//		count_str := strconv.Itoa(count)
		//		name=name+":"+count_str
		//		log.Info("name:",name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "ScrapeEventCount", "info"), "Oracle ScrapeEventCount.",
				[]string{"ScrapeEventCount_info"}, nil),
			prometheus.GaugeValue, count,
			name,
		)
	}
	return nil
}
func ScrapePct(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select round(sum(bytes_used)/(sum(bytes_used)+sum(bytes_free))*100,2) pct from v$temp_space_header")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var size float64
		if err := rows.Scan(&size); err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "Pct", "info"), "Oracle Pct.",
				[]string{}, nil),
			prometheus.GaugeValue,
			size,
		)
	}
	return nil
}

//select username,count(*) from v$session group by usrname;
//select username,count(username) from v$session where username is not null group by username;
func ScrapeUserConnectionsNum(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select username,count(username) from gv$session where username is not null group by username")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		var num float64
		//		var size_str string
		if err := rows.Scan(&name, &num); err != nil {
			return err
		}
		name = cleanName(name)
		//		size_str := strconv.FormatFloat(size, 'f', -1, 64)
		//		name=name+":"+size_str
		//		log.Info("name:",name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "ConnectionsNumber", "info"), "Oracle Connections.",
				[]string{"UserName"}, nil),
			prometheus.GaugeValue, num,
			name,
		)
	}
	return nil
}

func ScrapeUserConnStatus(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select username,status,count(*) from gv$session where username is not null group by username,status")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var name sql.NullString
		var status sql.NullString
		var num float64
		//		var size_str string
		if err := rows.Scan(&name, &status, &num); err != nil {
			return err
		}
		n := cleanName(name.String)
		//		size_str := strconv.FormatFloat(size, 'f', -1, 64)
		//		name=name+":"+size_str
		//		log.Info("name:",name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "UserConnectionsStatus", "info"), "Oracle user Connections status.",
				[]string{"UserName", "Status"}, nil),
			prometheus.GaugeValue, num,
			n, status.String,
		)
	}
	return nil
}

func ScrapeRowCache(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select name ,bytes/1024/1024 MB from v$sgastat where name like 'row cache%'")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		var num float64
		//		var size_str string
		if err := rows.Scan(&name, &num); err != nil {
			return err
		}
		name = cleanName(name)
		//		size_str := strconv.FormatFloat(size, 'f', -1, 64)
		//		name=name+":"+size_str
		//		log.Info("name:",name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "RowCache", "info"), "Oracle RowCache.",
				[]string{"row_cache_info"}, nil),
			prometheus.GaugeValue, num,
			name,
		)
	}
	return nil
}

//select name ,bytes/1024/1024 MB from v$sgastat where name like 'row cache%';
func ScrapeFsfi(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select tablespace_name,sqrt(max(blocks)/sum(blocks))*(100/sqrt(sqrt(count(blocks)))) FSFI from dba_free_space group by tablespace_name order by 1")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		var size float64
		//		var size_str string
		if err := rows.Scan(&name, &size); err != nil {
			return err
		}
		name = cleanName(name)
		//		size_str := strconv.FormatFloat(size, 'f', -1, 64)
		//		name=name+":"+size_str
		//		log.Info("name:",name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "FSFI", "info"), "Oracle FSFI.",
				[]string{"fsfi_info"}, nil),
			prometheus.GaugeValue, size,
			name,
		)
	}
	return nil
}

func ScrapeSessionNum(db *sql.DB, ch chan<- prometheus.Metric) error {
	var err error
	var SessionNum float64

	// There is probably a better way to do this with a single query. #FIXME when I figure that out.
	err = db.QueryRow("select count(*) from v$session").Scan(&SessionNum)
	if err != nil {
		return err
	}
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(prometheus.BuildFQName(namespace, "sessions", "num"),
			"Gauge metric with count of sessions", []string{}, nil),
		prometheus.GaugeValue,
		SessionNum,
	)

	return nil
}
func ScrapeDbVersion(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("select * from v$version")
	if err != nil {
		return err
	}
	//	var textItems = map[string]string{
	//		"version_info":         "",
	//}
	defer rows.Close()
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return err
		}
		value = cleanName(value)
		log.Infoln("value " + value)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "version", "info"), "Oracle version and distribution.",
				[]string{"version_info"}, nil),
			prometheus.GaugeValue, 1, value,
		)
	}
	return nil
}

// ScrapeSessions collects session metrics from the v$session view.
func ScrapeSessions(db *sql.DB, ch chan<- prometheus.Metric) error {
	var err error
	var activeCount float64
	var inactiveCount float64

	// There is probably a better way to do this with a single query. #FIXME when I figure that out.
	err = db.QueryRow("SELECT COUNT(*) FROM v$session WHERE status = 'ACTIVE'").Scan(&activeCount)
	if err != nil {
		return err
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(prometheus.BuildFQName(namespace, "sessions", "active"),
			"Gauge metric with count of sessions marked ACTIVE", []string{}, nil),
		prometheus.GaugeValue,
		activeCount,
	)

	err = db.QueryRow("SELECT COUNT(*) FROM v$session WHERE status = 'INACTIVE'").Scan(&inactiveCount)
	if err != nil {
		return err
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(prometheus.BuildFQName(namespace, "sessions", "inactive"),
			"Gauge metric with count of sessions marked INACTIVE.", []string{}, nil),
		prometheus.GaugeValue,
		inactiveCount,
	)

	return nil
}

// ScrapeWaitTime collects wait time metrics from the v$waitclassmetric view.
func ScrapeWaitTime(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT n.wait_class, round(m.time_waited/m.INTSIZE_CSEC,3) AAS from v$waitclassmetric  m, v$system_wait_class n where m.wait_class_id=n.wait_class_id and n.wait_class != 'Idle'")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var value float64
		if err := rows.Scan(&name, &value); err != nil {
			return err
		}
		name = cleanName(name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "wait_time", name),
				"Generic counter metric from v$waitclassmetric view in Oracle.", []string{}, nil),
			prometheus.CounterValue,
			value,
		)
	}
	return nil
}

// ScrapeActivity collects activity metrics from the v$sysstat view.
func ScrapeActivity(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT name, value FROM v$sysstat WHERE name IN ('parse count (total)', 'execute count', 'user commits', 'user rollbacks')")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var value float64
		if err := rows.Scan(&name, &value); err != nil {
			return err
		}
		name = cleanName(name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "activity", name),
				"Generic counter metric from v$sysstat view in Oracle.", []string{}, nil),
			prometheus.CounterValue,
			value,
		)
	}
	return nil
}

// Oracle gives us some ugly names back. This function cleans things up for Prometheus.
func cleanName(s string) string {
	s = strings.Replace(s, " ", "_", -1) // Remove spaces
	s = strings.Replace(s, "(", "", -1)  // Remove open parenthesis
	s = strings.Replace(s, ")", "", -1)  // Remove close parenthesis
	s = strings.Replace(s, "/", "", -1)  // Remove forward slashes
	s = strings.ToLower(s)
	return s
}

func main() {
	flag.Parse()
	log.Infoln("Starting oracledb_exporter " + Version)
	dsn := os.Getenv("DATA_SOURCE_NAME")
	//dsn = `system/1qaz@wsx#$@10.200.3.100`

	exporter := NewExporter(dsn)
	prometheus.MustRegister(exporter)
	http.Handle(*metricPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write(landingPage)
	})
	log.Infoln("Listening on", *listenAddress)
	if err := http.ListenAndServe(*listenAddress, nil); err != nil {
		log.Fatal(err)
	}
}
