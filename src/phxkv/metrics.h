#pragma once

#include <string>
#include <memory>

#include <prometheus/exposer.h>
#include <prometheus/registry.h>

extern std::vector<double> g_defaultBucket;

class TimerMetric
{
private:
  prometheus::Histogram *histogram_;
  prometheus::Family<prometheus::Histogram> *family_;

public:
  TimerMetric(prometheus::Family<prometheus::Histogram> *family , prometheus::Histogram *histogram)
  {
    family_ = family;
    histogram_ = histogram;
  }
  ~TimerMetric(){
    family_->Remove(histogram_);
  }
  void Observe(double value)
  {
    histogram_->Observe(value);
  };
};

class Guage
{
public:
  Guage(prometheus::Family<prometheus::Gauge> *family, prometheus::Gauge *gg)
  {
    family_ = family;
    guage_ = gg;
  }
  ~Guage(){
    family_->Remove(guage_);
  }

  void SetValue(double value)
  {
    guage_->Set(value);
  }

private:
  prometheus::Gauge *guage_;
  prometheus::Family<prometheus::Gauge> *family_;
};

class Counter
{
public:
  Counter(prometheus::Family<prometheus::Counter> *family, prometheus::Counter *cc)
  {
    family_ = family;
    counter_ = cc;
  }
  ~Counter() {
    family_->Remove(counter_);
  }
  void Increment(double value)
  {
    counter_->Increment(value);
  }
  
  void IncrementBy(){
    counter_->Increment();
  }

private:
  prometheus::Counter *counter_;
  prometheus::Family<prometheus::Counter> *family_;
};

class Metrics
{
public:
  Metrics(const std::string &listen);
  ~Metrics();
  TimerMetric *NewTimer(const std::string &name, const std::string &help, const std::map<std::string, std::string> &labels, std::vector<double> buckets = g_defaultBucket);
  Guage *NewGuage(const std::string &name, const std::string &help, const std::map<std::string, std::string> &labels);
  Counter *NewCounter(const std::string &name, const std::string &help, const std::map<std::string, std::string> &labels);

private:
  std::unique_ptr<prometheus::Exposer> exposer;
  std::shared_ptr<prometheus::Registry> registry;

};


extern Metrics *g_pmetrics;
