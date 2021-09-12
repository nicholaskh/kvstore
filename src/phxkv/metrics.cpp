#include "metrics.h"


Metrics::Metrics(const std::string &listen)
{
    exposer.reset(new prometheus::Exposer(listen, "/metrics"));

    registry = std::make_shared<prometheus::Registry>();

    exposer->RegisterCollectable(registry);
}

Metrics::~Metrics()
{
}

TimerMetric *Metrics::NewTimer(const std::string &name, const std::string &help, const std::map<std::string, std::string> &labels, std::vector<double> buckets)
{
    auto &family = prometheus::BuildHistogram()
                           .Name(name)
                           .Help(help)
                           .Labels(labels)
                           .Register(*registry);

    // @optimization : 这样有点危险，因为family是unique_ptr的管理对象,
    // 但是，这样保证Timer* 在 `prometheus::Registry` 前面释放就没有问题
    auto histogram1 = &family.Add({}, buckets);
    TimerMetric *timer = new TimerMetric(&family, histogram1);
    return timer;
}

Guage *Metrics::NewGuage(const std::string &name, const std::string &help, const std::map<std::string, std::string> &labels)
{
    auto &family = prometheus::BuildGauge()
                      .Name(name)
                      .Help(help)
                      .Labels(labels)
                      .Register(*registry);

    auto guage = &family.Add({});

    Guage *gg = new Guage(&family, guage);

    return gg;
}

Counter *Metrics::NewCounter(const std::string &name, const std::string &help, const std::map<std::string, std::string> &labels)
{
    auto &family = prometheus::BuildCounter()
                      .Name(name)
                      .Help(help)
                      .Labels(labels)
                      .Register(*registry);
    auto counter = &family.Add({});

    Counter *cc = new Counter(&family, counter);
    return cc;
}

Metrics *g_pmetrics;

std::vector<double> g_defaultBucket = {0.00001, 0.000025, 0.00005, 0.0001, 0.0005, 0.001, 0.003, .005, 0.007, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 60, 600};
