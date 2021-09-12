#ifndef __logger_h__
#define __logger_h__

#include <map>
#include <vector>
#include <stdio.h>
#include <pthread.h>
#include <sstream>
#include "datadef.h"
#include <syslog.h>

#define INFO_LOG(fmt, ...) \
	write_log(LOG_L_INFO, "%s:%d:%s: " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)

#define NOTICE_LOG(fmt, ...) \
	write_log(LOG_L_NOTICE, "%s:%d:%s: " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)

#define DEBUG_LOG(fmt, ...) \
	write_log(LOG_L_DEBUG, "%s:%d:%s: " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)

#define WARNING_LOG(fmt, ...) \
	write_log(LOG_L_WARNING, "%s:%d:%s: " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)

#define ERROR_LOG(fmt, ...) \
	write_log(LOG_L_ERROR, "%s:%d:%s: " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)

#define FATAL_LOG(fmt, ...) \
	write_log(LOG_L_FATAL, "%s:%d:%s: " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)

#define ALERT_LOG(fmt, ...) \
	write_log(LOG_L_ALERT, "%s:%d:%s: " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)

extern int g_enable_debuglog;
#define DEBUG(fmt, ...)                                                          \
	do                                                                             \
	{                                                                              \
		if (g_enable_debuglog)                                                       \
		{                                                                            \
			write_log(LOG_L_DEBUG, "%s:%d:%s: " fmt, __FILE__, __LINE__, __FUNCTION__, \
								##__VA_ARGS__);                                                  \
		}                                                                            \
	} while (0);

#ifdef _LOG_PERF_
#define TIMEDELTA(beg, end) \
	((end.tv_sec - beg.tv_sec) * 1000 + (end.tv_usec - beg.tv_usec) / 1000)
#define TIMEDELTA_USEC(beg, end) \
	((end.tv_sec - beg.tv_sec) * 1000 * 1000 + (end.tv_usec - beg.tv_usec))
#define LOG_PERF_DEF \
	struct timeval __tb__, __te__;
#define LOG_PERF_START \
	gettimeofday(&__tb__, 0);
#define LOG_PERF_LOG(prnt_freq, fmt, ...)                                                                                                     \
	do                                                                                                                                          \
	{                                                                                                                                           \
		static unsigned long long tsum = 0LL;                                                                                                     \
		static unsigned long long times = 0LL;                                                                                                    \
		gettimeofday(&__te__, 0);                                                                                                                 \
		tsum += TIMEDELTA_USEC(__tb__, __te__);                                                                                                   \
		times++;                                                                                                                                  \
		if (times % prnt_freq == 0)                                                                                                               \
		{                                                                                                                                         \
			write_log(LOG_L_NOTICE, "%s:Performance log::avg_time=[%d]us times[%llu], " fmt, __FUNCTION__, tsum / prnt_freq, times, ##__VA_ARGS__); \
			tsum = 0;                                                                                                                               \
		}                                                                                                                                         \
		gettimeofday(&__tb__, 0);                                                                                                                 \
	} while (0);
#define LOG_PERF_LOG_X(prnt_freq, min_msecs, fmt, ...)                                                                                        \
	do                                                                                                                                          \
	{                                                                                                                                           \
		static unsigned long long tsum = 0LL;                                                                                                     \
		static unsigned long long times = 0LL;                                                                                                    \
		gettimeofday(&__te__, 0);                                                                                                                 \
		tsum += TIMEDELTA_USEC(__tb__, __te__);                                                                                                   \
		times++;                                                                                                                                  \
		if (TIMEDELTA_USEC(__tb__, __te__) > (min_msecs)*1000)                                                                                    \
		{                                                                                                                                         \
			write_log(LOG_L_NOTICE, "%s:Performance log::BIG time=[%d]us, " fmt, __FUNCTION__, TIMEDELTA_USEC(__tb__, __te__), ##__VA_ARGS__);      \
		}                                                                                                                                         \
		if (times % prnt_freq == 0)                                                                                                               \
		{                                                                                                                                         \
			write_log(LOG_L_NOTICE, "%s:Performance log::avg_time=[%d]us times[%llu], " fmt, __FUNCTION__, tsum / prnt_freq, times, ##__VA_ARGS__); \
			tsum = 0;                                                                                                                               \
		}                                                                                                                                         \
		gettimeofday(&__tb__, 0);                                                                                                                 \
	} while (0);
#else
#define LOG_PERF_DEF \
	do                 \
	{                  \
	} while (0);
#define LOG_PERF(inf) \
	do                  \
	{                   \
	} while (0);
#define LOG_PERF_START \
	do                   \
	{                    \
	} while (0);
#define LOG_PERF_LOG \
	do                 \
	{                  \
	} while (0);
#endif
#define LOG_BUFF_SIZE (4096)

void write_log(log_level_t lvl, const char *fmt...);

class Logger
{
public:
	static int write_log_to_stderr(log_level_t lvl, const char *fmt...);

public:
	struct ThreadLogData
	{
		const int G_DATA_SIZE = 5 * 1024 * 1024;
		bool m_is_print;
		int m_data_size;
		int m_r_data_size;
		char *m_log_data;

		ThreadLogData()
		{
			m_is_print = true;
			m_data_size = G_DATA_SIZE;
			m_log_data = new char[m_data_size]();
			m_r_data_size = 0;
		}
		ThreadLogData(int data_size)
		{
			m_is_print = true;
			m_data_size = data_size;
			m_log_data = new char[m_data_size]();
			m_r_data_size = 0;
		}
		virtual ~ThreadLogData()
		{
			if (m_log_data)
			{
				delete[] m_log_data;
			}
		}

		int add_log(const std::string &log)
		{
			int esize = m_data_size - m_r_data_size;
			if (esize <= 0)
			{
				return -1;
			}

			int wsize =
					snprintf(m_log_data + m_r_data_size, esize, "%s", log.c_str());
			m_r_data_size += wsize;
			return 0;
		}

		void clear()
		{
			m_is_print = true;
			m_r_data_size = 0;
			m_log_data[0] = '\0';
		}
	};

public:
	Logger()
	{
		m_logfile = stderr;
		m_is_open_thread_log = false;
		m_thread_log_data = NULL;
		m_log_type = 0;
	}
	virtual ~Logger()
	{
		closelog();
	}

	/**
	 * fprintf_path：使用原始日志记录的文件路径；
	 * record_logl：日志记录的最小级别；
	 * log_out_type：日志输出形式（0:本地输出；1:远处输出；2:本地控制台和远处输出）
	 * log_type：使用何种方式记录日志（0:原始方式（注意：只能本地输出）；1：syslog方式）
	 * return：初始化成功与失败
	 **/
	bool init(const char *fprintf_path, log_level_t record_log_level, bool is_open_thread_log,
						uint8_t log_out_type, uint8_t log_type = 0);

	void writelog(log_level_t lvl, const char *fmt...);
	void writelog(log_level_t lvl, const char *fmt, va_list &va);
	void closelog();

	bool is_open_thread_log() const
	{
		return m_is_open_thread_log;
	}

	void set_thread_log_obj(void *log_data)
	{
		m_thread_log_data = log_data;
	}
	long interval(timeval &end, timeval &beg)
	{
		return (end.tv_sec - beg.tv_sec) * 1000 * 1000 + (end.tv_usec -
																											beg.tv_usec);
	}

	int push_thread_log(std::string &str, timeval &end, timeval &beg)
	{
		if (!m_is_open_thread_log)
		{
			return 0;
		}
		//    ThreadLogData* log_data = (ThreadLogData*)pthread_getspecific(m_thread_log_key);
		//    if(NULL == log_data) {
		//      log_data = new ThreadLogData();
		//      pthread_setspecific(m_thread_log_key, (void *)log_data);
		//    }

		ThreadLogData *log_data = (ThreadLogData *)m_thread_log_data;
		if (NULL == log_data)
		{
			//writelog(LOG_L_WARNING, "There is no m_thread_log_data.");
			return -1;
		}

		if (!log_data->m_is_print)
		{
			return 0;
		}

		long inter = interval(end, beg);
		std::ostringstream oss;
		oss << str << ",cost(us):" << inter << ";";
		log_data->add_log(oss.str());
		return 0;
	}

	void enable_thread_log(bool is_print)
	{
		ThreadLogData *log_data = (ThreadLogData *)m_thread_log_data;
		if (NULL == log_data)
		{
			//writelog(LOG_L_WARNING, "There is no m_thread_log_data.");
			return;
		}

		log_data->m_is_print = is_print;
	}

	void print_thread_log(bool is_real_print)
	{
		if (!m_is_open_thread_log)
		{
			return;
		}

		ThreadLogData *log_data = (ThreadLogData *)m_thread_log_data;
		if (NULL == log_data)
		{
			//writelog(LOG_L_WARNING, "There is no m_thread_log_data.");
			return;
		}

		if (log_data->m_is_print && is_real_print)
		{
			writelog(LOG_L_NOTICE, "%s", log_data->m_log_data);
		}
		log_data->clear();
	}

	log_level_t getLevel()
	{
		return m_loglevel;
	}

private:
	static int write_log(uint8_t type, FILE *fout, log_level_t lvl, const char *fmt, va_list &va);
	static int write_syslog(log_level_t lvl, const char *fmt, va_list &va);
	static int write_fprintf_log(FILE *fout, log_level_t lvl, const char *fmt, va_list &va);

	bool openlog(const char *path);

private:
	char logpath[1024];
	FILE *m_logfile;
	log_level_t m_loglevel;
	uint8_t m_log_type;

	static pthread_key_t m_thread_log_key;
	bool m_is_open_thread_log;

	static __thread void *m_thread_log_data;
};

extern Logger g_logger;
#endif
