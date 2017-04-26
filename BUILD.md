For developers of this toolkit:

The top-level build.xml contains two main targets:

* all - Builds and creates SPLDOC for the toolkit and samples. Developers should ensure this target is successful when creating a pull request.
* build-all-samples - Builds all samples. Developers should ensure this target is successful when creating a pull request.

# Prerequisite

* Set environment variable M2_HOME to the path of maven home directory.
* Download and build the [streamsx.inet](https://github.com/IBMStreams/streamsx.inet) toolkit.

