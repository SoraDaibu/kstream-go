| Case | Time after/before median (range) | B/op before → after | Allocs/op before → after |
|---|---|---|---|
| TypedProcessorDispatch/batch=1/cross-type=false | 1.009 (0.911–1.074) | 320 → 336 | 3 → 3 |
| TypedProcessorDispatch/batch=1/cross-type=true | 1.015 (0.914–1.068) | 320 → 336 | 3 → 3 |
| TypedProcessorDispatch/batch=256/cross-type=false | 1.029 (1.017–1.054) | 320 → 336 | 3 → 3 |
| TypedProcessorDispatch/batch=256/cross-type=true | 1.023 (0.992–1.039) | 320 → 336 | 3 → 3 |
| WindowExpiry/keys=64/size=1024 | 1.036 (1.018–1.067) | 879 → 895 | 21 → 21 |
| WindowExpiry/keys=64/size=1152921504606846976 | 1.026 (0.999–1.044) | 864 → 880 | 21 → 21 |
| WindowExpiry/keys=4096/size=1024 | 1.013 (0.995–1.075) | 1259 → 1277 | 21 → 21 |
| WindowExpiry/keys=4096/size=1152921504606846976 | 1.022 (1.005–1.097) | 907 → 923 | 21 → 21 |
