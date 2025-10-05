# Apache DataFu Release Guide

This document describes the process for creating a release of Apache DataFu.

## Prerequisites

* Java 8 or higher
* Gradle 9.x
* GPG key for signing releases
* Apache credentials for publishing

## Release Process

### 1. Prepare Release Environment

Set up your GPG environment. You may need to run this command to get GPG to show in the terminal.

    export GPG_TTY=`tty`

Bootstrap Gradle with the command below.  This creates the `gradlew` file referenced in these instructions.

    gradle -p . bootstrap.gradle

Make sure `changes.md` has been updated with all changes since the last release.

## Code Validation

### 2. Run Tests

Execute the full test suite to ensure everything is working:

    ./gradlew test

### 3. Check Code Style

Run the code style checks:

    ./gradlew check

### 4. Verify Dependencies

Check that all dependencies are properly resolved:

    ./gradlew dependencies

## Build Process

### 5. Clean Build

Perform a clean build:

    ./gradlew clean assemble

### 6. Run Integration Tests

Execute integration tests:

    ./gradlew integrationTest

## Documentation

### 7. Generate Documentation

Generate API documentation:

    ./gradlew javadoc

### 8. Update Website

Update the project website with new documentation and release notes.

## Release Artifacts

### 9. Create Source Distribution

Build the source distribution:

    ./gradlew sourceRelease

### 10. Sign Artifacts

Sign the release artifacts with your GPG key:

    gpg --armor --detach-sig apache-datafu-sources-*.tgz

### 11. Create Checksums

Generate checksums for the artifacts:

    sha512sum apache-datafu-sources-*.tgz > apache-datafu-sources-*.tgz.sha512

## Publishing

### 12. Upload to Apache

Upload the signed artifacts to the Apache repository:

    ./gradlew uploadArchives

### 13. Vote on Release

Start a vote on the Apache DataFu mailing list for the release.

### 14. Tag Release

Once the vote passes, tag the release:

    git tag -a datafu-x.y.z -m "Release Apache DataFu x.y.z"

### 15. Publish to Maven Central

Publish the artifacts to Maven Central:

    ./gradlew publishToMavenCentral

## Post-Release

### 16. Update Website

Update the project website with the new release information.

### 17. Announce Release

Send an announcement to the Apache DataFu mailing list and update the project website.

### 18. Prepare Next Release

Update version numbers and prepare for the next development cycle.

## Troubleshooting

### Common Issues

1. **GPG Signing Issues**: Ensure your GPG key is properly configured and the passphrase is available.

2. **Build Failures**: Check that all dependencies are available and the build environment is properly configured.

3. **Test Failures**: Ensure all tests pass before creating a release.

4. **Upload Issues**: Verify your Apache credentials and permissions.

### Getting Help

If you encounter issues during the release process:

1. Check the [Apache DataFu mailing list](http://datafu.apache.org/community/mailing-lists.html)
2. Review the [Apache DataFu documentation](http://datafu.apache.org/)
3. Open an issue on the [Apache DataFu JIRA](https://issues.apache.org/jira/browse/DATAFU)

## Release Checklist

- [ ] Update `changes.md` with all changes since last release
- [ ] Run full test suite (`./gradlew test`)
- [ ] Check code style (`./gradlew check`)
- [ ] Clean build (`./gradlew clean assemble`)
- [ ] Generate documentation (`./gradlew javadoc`)
- [ ] Create source distribution (`./gradlew sourceRelease`)
- [ ] Sign artifacts with GPG
- [ ] Generate checksums
- [ ] Upload to Apache repository
- [ ] Start release vote on mailing list
- [ ] Tag release in Git
- [ ] Publish to Maven Central
- [ ] Update website
- [ ] Send release announcement
- [ ] Prepare for next release

## Version Management

### Semantic Versioning

Apache DataFu follows semantic versioning (MAJOR.MINOR.PATCH):

- **MAJOR**: Incompatible API changes
- **MINOR**: New functionality in a backwards compatible manner
- **PATCH**: Backwards compatible bug fixes

### Version Updates

When updating version numbers:

1. Update version in `gradle.properties`
2. Update version in `build.gradle` files
3. Update documentation
4. Update website
5. Update release notes

## Security

### Security Releases

For security releases:

1. Follow the standard release process
2. Ensure all security fixes are properly tested
3. Coordinate with the Apache Security Team if necessary
4. Send security announcements to appropriate channels

### Vulnerability Reporting

To report security vulnerabilities:

1. Email security@apache.org
2. Do not disclose vulnerabilities publicly until they are fixed
3. Follow responsible disclosure practices

## Legal

### License Compliance

Ensure all code and dependencies comply with Apache licensing requirements:

1. Check all dependencies for license compatibility
2. Ensure all source code has proper Apache headers
3. Verify that all third-party code is properly attributed

### Trademark Usage

Follow Apache trademark guidelines when using Apache DataFu branding and logos.

## Support

### Release Support

For questions about the release process:

- [Apache DataFu Mailing List](http://datafu.apache.org/community/mailing-lists.html)
- [Apache DataFu Documentation](http://datafu.apache.org/)
- [Apache DataFu JIRA](https://issues.apache.org/jira/browse/DATAFU)

### Community

Join the Apache DataFu community:

- [Mailing Lists](http://datafu.apache.org/community/mailing-lists.html)
- [GitHub Repository](https://github.com/apache/datafu)
- [Website](http://datafu.apache.org/)