# ./lib/

## zstd-jni-1.5.2-4.jar

On-the-fly shading through `build.sbt` does not work for the `zstd-jni` library because of the included native libs.
Therefore, it was shaded manually and compiled on the target machine.

The original code lives in: https://github.com/luben/zstd-jni

The modified code can be found in: https://github.com/helgeho/zstd-jni