/*
Tencent is pleased to support the open source community by making 
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company. 
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may 
not use this file except in compliance with the License. You may 
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#include "util.h"
#include <unistd.h>
#include <stdio.h>
#include <sys/time.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <math.h>
#include <thread>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <dirent.h>

namespace phxpaxos {

using namespace std;

const  uint64_t Time ::GetTimestampSec()
{
    auto now_time = chrono::system_clock::now();
    uint64_t now = (chrono::duration_cast<chrono::seconds>(now_time.time_since_epoch())).count();
    return now;
}

const uint64_t Time :: GetTimestampMS() 
{
    auto now_time = chrono::system_clock::now();
    uint64_t now = (chrono::duration_cast<chrono::milliseconds>(now_time.time_since_epoch())).count();
    return now;
}

const uint64_t Time :: GetTimestampUS()
{
    auto now_time = chrono::system_clock::now();
    uint64_t now = (chrono::duration_cast<chrono::microseconds>(now_time.time_since_epoch())).count();
    return now;
}

const uint64_t Time :: GetSteadyClockMS() 
{
    auto now_time = chrono::steady_clock::now();
    uint64_t now = (chrono::duration_cast<chrono::milliseconds>(now_time.time_since_epoch())).count();
    return now;
}

void Time :: MsSleep(const int iTimeMs)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(iTimeMs));
}

/////////////////////////////////////////////
int FileUtils :: IsDir(const std::string & sPath, bool & bIsDir)
{
    bIsDir = false;
    struct stat tStat;
    int ret = stat(sPath.c_str(), &tStat);
    if (ret != 0)
    {
        return ret;
    }

    if (tStat.st_mode & S_IFDIR)
    {
        bIsDir = true;
    }

    return 0;
}

int FileUtils :: DeleteDir(const std::string & sDirPath)
{
    DIR * dir = nullptr;
    struct dirent  * ptr;

    dir = opendir(sDirPath.c_str());
    if (dir == nullptr)
    {
        return 0;
    }


    int ret = 0;
    while ((ptr = readdir(dir)) != nullptr)
    {
        if (strcmp(ptr->d_name, ".") == 0
                || strcmp(ptr->d_name, "..") == 0)
        {
            continue;
        }

        char sChildPath[1024] = {0};
        snprintf(sChildPath, sizeof(sChildPath), "%s/%s", sDirPath.c_str(), ptr->d_name);

        bool bIsDir = false;
        ret = FileUtils::IsDir(sChildPath, bIsDir);
        if (ret != 0)
        {
            break;
        }

        if (bIsDir)
        {
            ret = DeleteDir(sChildPath);
            if (ret != 0)
            {
                break;
            }
        }
        else
        {
            ret = remove(sChildPath);
            if (ret != 0)
            {
                break;
            }
        }
    }

    closedir(dir);

    if (ret == 0)
    {
        ret = remove(sDirPath.c_str());
    }

    return ret;
}
int FileUtils :: DeleteDirFile(const std::string & sDirPath)
{
    DIR * dir = nullptr;
    struct dirent  * ptr;

    dir = opendir(sDirPath.c_str());
    if (dir == nullptr)
    {
        return 0;
    }

    int ret = 0;//代表成功
    while ((ptr = readdir(dir)) != nullptr)
    {
        if (strcmp(ptr->d_name, ".") == 0
                || strcmp(ptr->d_name, "..") == 0)
        {
            continue;
        }

        char sChildPath[1024] = {0};
        snprintf(sChildPath, sizeof(sChildPath), "%s/%s", sDirPath.c_str(), ptr->d_name);

        bool bIsDir = false;
        ret = FileUtils::IsDir(sChildPath, bIsDir);
        if (ret != 0)
        {
            break;
        }

        if (bIsDir)
        {
            ret = DeleteDir(sChildPath);
            if (ret != 0)
            {
                break;
            }
        }
        else
        {
            ret = remove(sChildPath);
            if (ret != 0)
            {
                break;
            }
        }
    }
    closedir(dir);

    return ret;
}
int FileUtils :: IterDir(const std::string & sDirPath, std::vector<std::string> & vecFilePathList)
{
    DIR * dir = nullptr;
    struct dirent  * ptr;

    dir = opendir(sDirPath.c_str());
    if (dir == nullptr)
    {
        return 0;
    }


    int ret = 0;
    while ((ptr = readdir(dir)) != nullptr)
    {
        if (strcmp(ptr->d_name, ".") == 0
                || strcmp(ptr->d_name, "..") == 0)
        {
            continue;
        }

        char sChildPath[1024] = {0};
        snprintf(sChildPath, sizeof(sChildPath), "%s/%s", sDirPath.c_str(), ptr->d_name);

        bool bIsDir = false;
        ret = FileUtils::IsDir(sChildPath, bIsDir);
        if (ret != 0)
        {
            break;
        }

        if (bIsDir)
        {
            ret = IterDir(sChildPath, vecFilePathList);
            if (ret != 0)
            {
                break;
            }
        }
        else
        {
            vecFilePathList.push_back(sChildPath);
        }
    }

    closedir(dir);

    return ret;
}

int FileUtils :: IterFile(const std::string & sDirPath, std::vector<std::string> & vecFilePathList)
{
    DIR * dir = nullptr;
    struct dirent  * ptr;

    dir = opendir(sDirPath.c_str());
    if (dir == nullptr)
    {
        return 0;
    }


    int ret = 0;
    while ((ptr = readdir(dir)) != nullptr)
    {
        if (strcmp(ptr->d_name, ".") == 0
                || strcmp(ptr->d_name, "..") == 0)
        {
            continue;
        }

        char sChildPath[1024] = {0};
        snprintf(sChildPath, sizeof(sChildPath), "%s",  ptr->d_name);

        vecFilePathList.push_back(sChildPath);

    }

    closedir(dir);

    return ret;
}


int FileUtils::GetdirSize(const std::string & sDirPath)
{
    int totalSize=0;
    if(sDirPath.empty() ){
        return 0;
    }
    DIR *dp;
    struct dirent *entry;
    struct stat statbuf;
    if( (dp=opendir(sDirPath.c_str() ) )==NULL){
        return -1;
    }

    while( (entry=readdir(dp))!=NULL ){
        char subdir[256];
        sprintf(subdir,entry->d_name);
        lstat(subdir,&statbuf );
        if(S_ISDIR(statbuf.st_mode) ){

            if(strcmp(".",entry->d_name) ==0 || strcmp("..",entry->d_name)==0 ){
                continue;
            }else{
                int tempsize = GetdirSize(subdir);
                totalSize+=tempsize;
            }
        }else{
            totalSize+= statbuf.st_size;
        }
    }
    closedir(dp);
    return totalSize/1024/1024; //bytes====M

}

int FileUtils::CreateDir(const std::string & sDirPath, mode_t mode)
{
    struct stat st;
  int status = 0;

  if (stat(sDirPath.c_str(), &st) != 0) {
    if (mkdir(sDirPath.c_str(), mode) != 0 && errno != EEXIST)
      status = -1;
  } else if (!S_ISDIR(st.st_mode)) {
    errno = ENOTDIR;
    status = -1;
  }

  return (status);
}
////////////////////////////////

TimeStat :: TimeStat()
{
    m_llTime = Time::GetSteadyClockMS();
}

int TimeStat :: Point()
{
    uint64_t llNowTime = Time::GetSteadyClockMS();
    int llPassTime = 0;
    if (llNowTime > m_llTime)
    {
        llPassTime = llNowTime - m_llTime;
    }

    m_llTime = llNowTime;

    return llPassTime;
}

/////////////////////////////////

uint64_t OtherUtils :: GenGid(const uint64_t llNodeID)
{
    return (llNodeID ^ FastRand()) + FastRand();
}

//////////////////////////////////////////////////////////

#ifdef __i386

__inline__ uint64_t rdtsc()
{
    uint64_t x;
    __asm__ volatile ("rdtsc" : "=A" (x));
    return x;
}

#elif __amd64

__inline__ uint64_t rdtsc()
{

    uint64_t a, d;
    __asm__ volatile ("rdtsc" : "=a" (a), "=d" (d));
    return (d<<32) | a;
}

#endif

struct FastRandomSeed {
    bool init;
    unsigned int seed;
};

static __thread FastRandomSeed seed_thread_safe = { false, 0 };

static void ResetFastRandomSeed()
{
    seed_thread_safe.seed = rdtsc();
    seed_thread_safe.init = true; 
}

static void InitFastRandomSeedAtFork()
{
    pthread_atfork(ResetFastRandomSeed, ResetFastRandomSeed, ResetFastRandomSeed);
}

static void InitFastRandomSeed()
{
    static pthread_once_t once = PTHREAD_ONCE_INIT;
    pthread_once(&once, InitFastRandomSeedAtFork);

    ResetFastRandomSeed();
}

const uint32_t OtherUtils :: FastRand()
{
    if (!seed_thread_safe.init)
    {
        InitFastRandomSeed();
    }

    return rand_r(&seed_thread_safe.seed);
}


void StringUtil::splitstr(const string & str,const string & param,std::vector<string> &ret )
{

	if ("" == str)
    {
        return;
    }
    //方便截取最后一段数据
    std::string strs = str + param;
    
    size_t pos = strs.find(param);
    size_t size = strs.size();

    while (pos != std::string::npos)
    {
        std::string x = strs.substr(0,pos);
        if( !x.empty() ){
            ret.push_back(x);
        }
        strs = strs.substr(pos+1,size);
        pos = strs.find(param);
    }
    return ;
}

bool StringFormat::StrIsInt(const string value )
{
    int size = value.length();
    int pos=0;
    int flag=1;//代表大于0，否则小于0
    while( pos < size ){
        char key = value.at(pos );
        if ( key == '-' && pos==0 ){
            flag=0;
        }else{
            if( key<'0' || key>'9' ){
                return false;
            }
        }
        pos++;
    }
    return true;
}

bool StringFormat::StrIsFloat(const string value )
{
    int size = value.length();
    int pos=0;
    int flag=1;//代表大于0，否则小于0
    while( pos < size ){
        char key = value.at(pos );
        if (  pos==0 && key == '-'  ){
            flag=0;
        }else{
            if(  (key<'0' || key>'9') && key!='.' ){
                return false;
            }
        }
        pos++;
    }
    return true;
}

string StringFormat::IntToStr(const int val)
{
    stringstream ss;
    ss<<val;
    return ss.str();
}
string StringFormat::FloatToStr(const float val)
{
    stringstream ss;
    ss<<val;
    return ss.str();
}
}


