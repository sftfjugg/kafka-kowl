---
title: Installation & Deployment
path: /docs/installation
---

# Installation & Deployment

This page describes how to install Redpanda Console on your platform. This documentation targets the installation
of Redpanda Console without Redpanda or Apache Kafka. Redpanda maintains documentation that is written
for the installation of Redpanda Platform which includes Redpanda Console here: https://docs.redpanda.com/docs/deploy/.

## Docker images

Docker images are available on [Dockerhub](https://hub.docker.com/r/redpandadata/console/tags). We comply with semantic versioning and every new release is pushed to Dockerhub.
Additionally we publish a new docker image after each push to any branch to a separate [Dockerhub repository](https://hub.docker.com/r/redpandadata/console-unstable/tags).

## Binaries

Precompiled binaries for different platforms are attached to each release.

## Deployment

While RP Console was written to be deployed in Kubernetes it does not have any specific dependencies. 
You can run it with any other container orchestrator or on bare metal.

### Hosting and Proxies (path rewriting)

If you want to host Console under a sub path (e.g. `domain.com/some/sub/path` instead of `domain.com/`) 
take a look at [HTTP Path Rewrites](https://docs.redpanda.com/docs/manage/console/http-path-rewrites/) for more information.

### Kubernetes
You can use the Redpanda Console Helm chart to deploy Redpanda Console into your Kubernetes cluster. 
To install the Helm repository and chart, run:

```bash
helm repo add redpanda 'https://charts.redpanda.com/' 
helm repo update
helm install redpanda/console -f myvalues.yaml
```

We maintain a [Helm chart](https://github.com/redpanda-data/helm-charts/tree/main/charts/console) which makes 
it easy to deploy Console on Kubernetes. Please refer to this repo for further documentation and/or help.

If you want to create your own manifest you can browse through the Helm chart linked above.

### Configuration

Console can be configured using environment variables and a YAML config. To use YAML you need to set the path to 
the YAML config either using the flag `-config.filepath` or by setting the env variable `CONFIG_FILEPATH`. 
Additionally, you can use flags for all sensitive input (such as Kafka passwords). 
All available flags along with a reference YAML config are described below.

If your Kafka cluster requires TLS authentication you can make certificates available via volume mounts. 
Afterwards you need to configure the paths to your certificates in the YAML config as shown in the 
reference config (see section [config files](#config-files)).

#### Environment variables

Everything that can be configured via YAML can also be configured using env variables. 
Due to the complexity of the config we recommend using YAML configs for most deployments, 
but you can also use env variables which might be handy to start Console within a few secs 
by executing a one line cli statement.

All environment variable keys are generated by using the YAML config names as described 
in the [reference config](/docs/config/console.yaml).

#### Flags

In general we only use flags to specify the path to your config file which contains 
all the actual configuration. Because Console requires sensitive information such as 
credentials, we offer further flags so that you don't need to put them into your 
YAML configuration. This way the YAML config can be pushed into your repository 
while sensitive information can be mounted during the deployment process.

| Argument | Description | Default |
| --- | --- | --- |
| --config.filepath | Path to the config file | (No default) |
| --kafka.sasl.password | SASL Password | (No default) |
| --kafka.sasl.gssapi.password | Kerberos password if auth type user auth is used | (No default) |
| --kafka.tls.passphrase | Passphrase to decrypt the TLS key (leave empty for unencrypted key files) | (No default) |

#### Config Files

All available settings which can be configured in your YAML configs can be found here and should document themselves:

[/docs/config/console.yaml](https://github.com/redpanda-data/console/blob/master/docs/config/console.yaml)
