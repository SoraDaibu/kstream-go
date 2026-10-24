# Go toolchain baseline

Use Go 1.27. The module requires language/toolchain version 1.27.0 and suggests
Go 1.27.3 via its toolchain directive. On 2026-10-24, Go 1.27.3 is the latest
stable patch listed by the [official download feed](https://go.dev/dl/?mode=json)
and [release history](https://go.dev/doc/devel/release#go1.27).

Ordinary Go/differential CI follows the latest 1.27 patch. Broker, storage and
benchmark jobs pin 1.27.3 so their measurements are reproducible. Future patch
updates need the same correctness and benchmark gates. Record the exact compiler
and dependency versions in every retained measurement.

Historical reports produced using Go 1.23.12 remain unchanged as evidence of
those runs. They are not measurements of the Go 1.27 implementation. Rerun both
sides of any Go before/after optimization using the same 1.27.3 toolchain; do not
attribute compiler/runtime changes to application code.
