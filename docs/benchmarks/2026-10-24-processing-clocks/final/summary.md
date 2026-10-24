| Case | Time after/before median (range) | B/op before → after | Allocs/op before → after |
|---|---|---|---|
| TypedProcessorDispatch/batch=1/cross-type=false | 0.947 (0.853–0.990) | 320 → 256 | 3 → 2 |
| TypedProcessorDispatch/batch=1/cross-type=true | 0.929 (0.900–1.002) | 320 → 256 | 3 → 2 |
| TypedProcessorDispatch/batch=256/cross-type=false | 0.981 (0.887–0.989) | 320 → 256 | 3 → 2 |
| TypedProcessorDispatch/batch=256/cross-type=true | 0.988 (0.882–1.006) | 320 → 256 | 3 → 2 |
| WindowExpiry/keys=64/size=1024 | 1.008 (0.902–1.031) | 880 → 816 | 21 → 20 |
| WindowExpiry/keys=64/size=1152921504606846976 | 1.019 (0.927–1.033) | 864 → 800 | 21 → 20 |
| WindowExpiry/keys=4096/size=1024 | 1.027 (0.960–1.118) | 1260 → 1194 | 21 → 20 |
| WindowExpiry/keys=4096/size=1152921504606846976 | 0.998 (0.966–1.032) | 907 → 843 | 21 → 20 |
