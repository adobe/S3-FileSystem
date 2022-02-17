## Contributing Guide

Thanks for choosing to contribute!

The following are a set of guidelines to follow when contributing to this project.

* **Stable branch**: _main_

## Code of conduct

This project adheres to the Adobe [code of conduct](../CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.
Please report unacceptable behavior to [Grp-opensourceoffice@adobe.com](mailto:Grp-opensourceoffice@adobe.com).

## Contributor License Agreement

All third-party contributions to this project must be accompanied by a signed contributor license. 
This gives Adobe permission to redistribute your contributions as part of the project. Sign our [CLA](http://adobe.github.io/cla.html). 
You only need to submit an Adobe CLA one time, so if you have submitted one previously, you are good to go!

## Design doc

For a better and quicker understanding of the codebase, please read the design [docs](./Design.md)   

## Code Reviews

All submissions should come in the form of pull requests and need to be reviewed by project committers.
Read [GitHub's pull request documentation](https://help.github.com/articles/about-pull-requests/) for more information on sending pull requests.

## Coding Guidelines

- **Avoid** introducing state. The state of the filesystem should always be read from the meta store.
- For POJO like classes please use **Immutables** library.
- For collections / data structures / basic algorithms use provided `JDK`, `Guava`.
- Avoid adding external dependencies unless absolutely necessary.
   * All the dependencies get bundled and shaded into the `s3-fs.jar`.
   * Adding un-necessary dependencies bloats the jar.
   * Always prefer implementing trivial/simple pieces of code in the project (e.g. an `ImmutableTriple`).
- JavaDoc **SHOULD** exist on API boundaries between components.
- JavaDoc **SHOULD** exist on internal functionalities that warrant it (i.e. slightly more complex algorithms).
- Use `slf4j` API instead of using `log4j` directly (`log4j` is the logging implementation for `slf4j`).


## Integration tests

The integration tests use [localstack](https://github.com/localstack/localstack) to emulate cloud services such as S3 and DynamoDB.
To run integration tests locally you need to have [docker](https://docs.docker.com/docker-for-mac/install/) installed.