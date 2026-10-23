# CoEvolution Communication Infrastructure

> Event backbone of the CoEvolution Hub (T6.1, AEGIS). **Kafka** delivers events from the Orchestrator to the CoEvolution engines, so that each engine starts its work as soon as its input is ready.

[![Kafka](https://img.shields.io/badge/Kafka-messaging-orange)](https://kafka.apache.org)
[![ZooKeeper](https://img.shields.io/badge/ZooKeeper-coordination-yellow)](https://zookeeper.apache.org)
[![Kafdrop](https://img.shields.io/badge/Kafdrop-topic%20viewer-lightgrey)](https://github.com/obsidiandynamics/kafdrop)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-deployment-blue)](https://kubernetes.io)

---

## Table of Contents

- [Overview](#overview)
- [Why this repository contains no source code](#why-this-repository-contains-no-source-code)
- [Architecture](#architecture)
- [Components](#components)
- [Role in the Testbed](#role-in-the-testbed)
- [Kafka Topics](#kafka-topics)
- [Event Structure](#event-structure)
- [Repository Structure](#repository-structure)
- [Deployment](#deployment)
- [Access](#access)
- [Related Repositories](#related-repositories)

---

## Overview

The CoEvolution Hub combines three communication patterns, each used where it fits best:

| Pattern | Used for |
|---------|----------|
| **REST** | Requests from the Dashboard and the engines to the Orchestrator: registering models, uploading and downloading files, requesting an AIMBOM |
| **Kafka (publish/subscribe)** | Notifications from the Orchestrator to the engines that new input is ready to process |
| **Object exchange** | Files stored in the Knowledge Base and retrieved through the Orchestrator |

**Why Kafka.** With publish/subscribe, the Orchestrator publishes an event to a named topic without needing to know which engine is listening, or whether it is available at that moment. Each engine subscribes to the topics it needs and processes events at its own pace. The Hub and the engines are therefore **decoupled in time**: an engine that is temporarily unavailable does not cause a failure, and it receives the event once it reconnects. A direct REST call to an unavailable engine would simply fail.

## Why this repository contains no source code

Apache Kafka, ZooKeeper and Kafdrop are mature, widely adopted open-source products and are used **without modification**. The CoEvolution-specific logic is implemented elsewhere:

- **Producing events** (when to publish and what each event contains) is implemented in the **[Orchestrator](https://github.com/CoEvolution-Project-EU/Orchestrator_T6.1)**.
- **Consuming events** is implemented in each engine, on its own premises.

What is specific to CoEvolution at this layer is **how the broker is deployed and configured**. This repository therefore contains the **Kubernetes manifests** that define the Communication Infrastructure on the CoEvolution testbed: broker configuration, listeners, persistent storage and services. Because they are version-controlled, the infrastructure can be redeployed in the same way on any Kubernetes cluster.

## Architecture

```
┌───────────────────── CoEvolution central server ─────────────────────┐
│                                                                      │
│   Orchestrator ──publish──►  ┌─────────┐       ┌───────────┐         │
│                              │  Kafka  │◄─────►│ ZooKeeper │         │
│   Kafdrop ─────inspect─────► └────┬────┘       └───────────┘         │
│                                   │                                  │
└───────────────────────────────────┼──────────────────────────────────┘
                                    │ subscribe
               ┌────────────────────┼────────────────────┐
               ▼                    ▼                    ▼
            RAGRE                  STA                 STRAE
                        (engine partner premises)
```

The broker exposes two listeners:

- an **internal listener**, used by Hub components inside the cluster, such as the Orchestrator and Kafdrop;
- an **external listener**, used by the engines that run on partner infrastructure.

Each client is given the address that is reachable from its own network, so both groups can connect to the same broker.

## Components

| Component | Role |
|-----------|------|
| **Kafka** | Message broker: stores the events of each topic and delivers them to subscribers |
| **ZooKeeper** | Coordination service holding the broker's metadata (brokers, topics, partitions) |
| **Kafdrop** | Web interface for inspecting topics, messages and consumer groups. It is an operational tool for developers and integrators; no component consumes from it. |

## Role in the Testbed

The Communication Infrastructure runs on the central CoEvolution server, together with the Dashboard, the Orchestrator, User and Credential Management, and the Knowledge Base. The engines run on their partners' infrastructure and connect to the broker over the network.

It drives the asynchronous steps of the platform's core flows:

| Flow | Event |
|------|-------|
| Model Registration | RAGRE is notified that a new model's requirements are ready for risk assessment |
| Assessment and Report Generation | The Security Testing/Assessment Engine is notified that a user has submitted a new assessment or defence result |
| AIMBOM Generation and Trusted Model Registration | STRAE is notified that a model's evidence is ready for the AIMBOM to be generated or updated |

## Kafka Topics

| Topic | Published by the Orchestrator when | Consumer |
|-------|------------------------------------|----------|
| `ragre_models` | A model's requirements file is uploaded | RAGRE |
| `assessment_reports` | A user uploads an assessment or defence result. A flag in the event distinguishes the two. | Security Testing/Assessment Engine |
| `strae` | AIMBOM generation is requested for a model version | STRAE |

## Event Structure

Events carry **metadata and download links, not files**. Every event identifies the model and version it concerns and lists the related files:

```json
{
  "event": "ragre_models",
  "model_name": "<model name>",
  "model_uuid": "<model identifier>",
  "version": "<version>",
  "uploadedBy": "<user>",
  "uploadedAt": "<timestamp>",
  "files": [
    {
      "_id": "<file identifier>",
      "bucket": "<bucket>",
      "filename": "<filename>",
      "download_url": "<link to the file through the Orchestrator>"
    }
  ]
}
```

The engine downloads the content through the Orchestrator's authenticated REST API. Large artifacts therefore never pass through the broker, and file access stays under the platform's access control.

## Repository Structure

```
├── zookeeper-pvc.yaml          # ZooKeeper persistent storage
├── zookeeper-deployment.yaml   # ZooKeeper server
├── zookeeper-service.yaml      # ZooKeeper service
├── kafka-pvc.yaml              # Kafka persistent storage (topic data)
├── kafka-deployment.yaml       # Kafka broker and listener configuration
├── kafka-service.yaml          # Internal and external Kafka services
├── kafdrop-deployment.yaml     # Kafdrop web interface
└── kafdrop-service.yaml        # Kafdrop service
```

## Deployment

**Prerequisites:** a Kubernetes cluster, `kubectl`, a default StorageClass, and an ingress controller for Kafdrop.

Components are deployed in dependency order: Kafka needs ZooKeeper, and Kafdrop needs Kafka.

```bash
kubectl apply -f zookeeper-pvc.yaml -f zookeeper-deployment.yaml -f zookeeper-service.yaml
kubectl apply -f kafka-pvc.yaml -f kafka-deployment.yaml -f kafka-service.yaml
kubectl apply -f kafdrop-deployment.yaml -f kafdrop-service.yaml
```

**Verify:**

```bash
kubectl get pods
kubectl get svc
```

All pods should be `Running`, and the broker should appear in Kafdrop.

**Design choices in the manifests:**

- **Persistent volumes:** events and broker metadata survive pod restarts.
- **Separate internal and external listeners:** Hub components and remote engines each connect through an address that is reachable from their own network.
- **Kafdrop served only through the API gateway:** it has no service exposed directly, so there is a single external entry point.

## Access

| Service | Access |
|---------|--------|
| Kafka broker | Internal listener for Hub components; external listener for the engines |
| Kafdrop | `https://aegis.coevolution-project.eu/kafdrop`, for developers and integrators |

## Related Repositories

| Repository | Description |
|------------|-------------|
| [Orchestrator](https://github.com/CoEvolution-Project-EU/Orchestrator_T6.1) | Backend of the Hub and the producer of all events |
| Knowledge Base | MinIO and MongoDB storage for files and metadata |

---

## License

CoEvolution Project EU

## Author

**Konstantinos Krouti**
GitHub: (https://github.com/kostaskr13)
