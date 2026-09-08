# Linkerd Proxy

Linkerd's Rust proxy provides transparent HTTP, HTTP/2, TCP, and WebSocket proxying, load balancing, mutual TLS, and Prometheus metrics. It primarily runs in Linux containers.

## Build and test

Use the included [Dev Container](.devcontainer) or install the Rust toolchain and [just-cargo](https://github.com/linkerd/dev/tree/main/bin/just-cargo).

```sh
just build
just test
just docker
```

## Code

- [Proxy executable](linkerd2-proxy)
- [Application crates](linkerd/app)
- [Integration tests](linkerd/app/integration)

See the [Linkerd project](https://github.com/linkerd/linkerd2), [code of conduct](https://github.com/linkerd/linkerd/wiki/Linkerd-code-of-conduct), and [fuzzing documentation](docs/FUZZING.md).

## License

linkerd2-proxy is copyright 2018 the linkerd2-proxy authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
these files except in compliance with the License. You may obtain a copy of the
License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
