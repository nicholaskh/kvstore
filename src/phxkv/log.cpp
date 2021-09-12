#include <stdarg.h>
#include <time.h>
#include <string.h>

#include "log.h"

int fprintfToFile(FILE *fout, log_level_t lvl, const char *fmt, va_list &va)
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
    case LOG_L_ALERT:
        strcpy(buff + bpos, "ALERT: ");
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

void stderrprintf(const char *fmt...)
{
    va_list args;
    va_start(args, fmt);
    fprintfToFile(stderr, LOG_L_ERROR, fmt, args);
    va_end(args);
}
