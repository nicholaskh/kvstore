#pragma once

#include <future>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "prometheus/registry.h"

namespace prometheus {

class Gateway {
 public:
  using Labels = std::map<std::string, std::string>;

  Gateway(const std::string& uri, const std::string jobname,
          const Labels& labels = {}, const std::string username = {},
          const std::string password = {});
  ~Gateway();

  void RegisterCollectable(const std::weak_ptr<Collectable>& collectable,
                           const Labels* labels = nullptr);

  static const Labels GetInstanceLabel(std::string hostname);

  enum class HttpMethod {
    Post,
    Put,
    Delete,
  };

  // Push metrics to the given pushgateway.
  int Push() { return push(HttpMethod::Post); }

  std::future<int> AsyncPush() { return async_push(HttpMethod::Post); }

  // PushAdd metrics to the given pushgateway.
  int PushAdd() { return push(HttpMethod::Put); }

  std::future<int> AsyncPushAdd() { return async_push(HttpMethod::Put); }

  // Delete metrics from the given pushgateway.
  int Delete();

  // Delete metrics from the given pushgateway.
  std::future<int> AsyncDelete();

 private:
  std::string jobUri_;
  std::string labels_;
  std::string auth_;

  using CollectableEntry = std::pair<std::weak_ptr<Collectable>, std::string>;
  std::vector<CollectableEntry> collectables_;

  std::string getUri(const CollectableEntry& collectable) const;

  int performHttpRequest(HttpMethod method, const std::string& uri,
                         const std::string& body) const;

  int push(HttpMethod method);

  std::future<int> async_push(HttpMethod method);
};

}  // namespace prometheus
