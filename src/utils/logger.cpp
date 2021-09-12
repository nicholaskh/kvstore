#include <stdarg.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include "logger.h"

Logger g_logger;
__thread void *Logger::m_thread_log_data = NULL;

void write_log(log_level_t lvl, const char *fmt...)
{
	va_list args;
	va_start(args, fmt);
	g_logger.writelog(lvl, fmt, args);
	va_end(args);
}

int Logger::write_log_to_stderr(log_level_t lvl, const char *fmt...)
{
	int ret = 0;
	va_list args;
	va_start(args, fmt);
	ret = Logger::write_fprintf_log(stderr, lvl, fmt, args);
	va_end(args);
	return ret;
}

int Logger::write_log(uint8_t type, FILE *fout, log_level_t lvl, const char *fmt, va_list &va)
{
	if (0 == type)
	{
		return write_fprintf_log(fout, lvl, fmt, va);
	}

	return write_syslog(lvl, fmt, va);
}

int Logger::write_fprintf_log(FILE *fout, log_level_t lvl, const char *fmt, va_list &va)
{
	int ret = 0;
	if (NULL == fout || NULL == fmt)
	{
		return ret;
	}

	char buff[LOG_BUFF_SIZE];
	int bpos = 0;
	char strtime[100];
	struct tm otm;
	time_t lt;
	time(&lt);
	localtime_r(&lt, &otm);
	strftime(strtime, sizeof(strtime), "%Y-%m-%d %T", &otm);
	//  va_list args;
	//  va_start(args, fmt);
	buff[0] = '\0';
	switch (lvl)
	{
	case LOG_L_INFO:
		strcpy(buff + bpos, "INFO: ");
		break;
	case LOG_L_DEBUG:
		strcpy(buff + bpos, "DEBUG: ");
		break;
	case LOG_L_NOTICE:
		strcpy(buff + bpos, "NOTICE: ");
		break;
	case LOG_L_WARNING:
		strcpy(buff + bpos, "WARNING: ");
		break;
	case LOG_L_ERROR:
		strcpy(buff + bpos, "ERROR: ");
		break;
	case LOG_L_FATAL:
		strcpy(buff + bpos, "FATAL: ");

		break;
	}
	bpos += strlen(buff + bpos);
	sprintf(buff + bpos, "%s ", strtime);
	bpos += strlen(buff + bpos);
	sprintf(buff + bpos, "(tid=%lx): ", pthread_self());
	bpos += strlen(buff + bpos);
	sprintf(buff + bpos, "%ld: ", time(NULL));
	bpos += strlen(buff + bpos);
	vsnprintf(buff + bpos, sizeof(buff) - bpos, fmt, va);
	//  va_end(args);
	fprintf(fout, "%s\n", buff);
	fflush(fout);

	return ret;
}

int Logger::write_syslog(log_level_t lvl, const char *fmt, va_list &va)
{
	switch (lvl)
	{
	case LOG_L_DEBUG:
		vsyslog(LOG_DEBUG, fmt, va);
		break;
	case LOG_L_INFO:
		vsyslog(LOG_INFO, fmt, va);
		break;
	case LOG_L_NOTICE:
		vsyslog(LOG_NOTICE, fmt, va);
		break;
	case LOG_L_WARNING:
		vsyslog(LOG_WARNING, fmt, va);
		break;
	case LOG_L_ERROR:
		vsyslog(LOG_ERR, fmt, va);
		break;
	case LOG_L_FATAL:
		vsyslog(LOG_CRIT, fmt, va);
		break;
	case LOG_L_ALERT:
		vsyslog(LOG_ALERT, fmt, va);
		break;
	}

	return 0;
}

bool Logger::init(const char *fprintf_path, log_level_t record_log_level, bool is_open_thread_log,
				  uint8_t log_out_type, uint8_t log_type)
{
	bool ret = true;
	m_log_type = log_type;
	m_loglevel = record_log_level;
	m_is_open_thread_log = is_open_thread_log;

	if (0 == log_type)
	{
		ret = openlog(fprintf_path);
	}
	else
	{
		// syslog
		if (0 == log_out_type)
		{
			::openlog(0, LOG_CONS | LOG_PID | LOG_NDELAY, LOG_USER);
		}
		else if (1 == log_out_type)
		{
			::openlog(0, LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL6);
		}
		else
		{
			::openlog(0, LOG_CONS | LOG_PID | LOG_PERROR | LOG_NDELAY, LOG_LOCAL6);
		}
	}

	return ret;
}

void Logger::writelog(log_level_t lvl, const char *fmt, va_list &va)
{
	if (lvl < m_loglevel)
	{
		return;
	}
	write_log(m_log_type, m_logfile, lvl, fmt, va);
}

void Logger::writelog(log_level_t lvl, const char *fmt...)
{
	if (lvl < m_loglevel)
	{
		return;
	}
	va_list args;
	va_start(args, fmt);
	write_log(m_log_type, m_logfile, lvl, fmt, args);
	va_end(args);
}

bool Logger::openlog(const char *path)
{
	bool bret = false;
	strcpy(logpath, path);
	m_logfile = fopen(path, "a");
	if (m_logfile != NULL)
	{
		bret = true;
	}
	else
	{
		bret = false;
	}
	return bret;
}

void Logger::closelog()
{
	writelog(LOG_L_NOTICE, "closelog");
	if (m_logfile)
	{
		fclose(m_logfile);
	}
	m_logfile = NULL;

	if (0 != m_log_type)
	{
		::closelog();
	}
}
