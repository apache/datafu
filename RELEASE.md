Auto-generate all missing headers in files:

    ./gradlew licenseFormatMain
    ./gradlew licenseFormatTest

Check to see if any files were modified to add the missing headers.  If so be sure to commit the changes before proceeding.

Build the code, run the tests, and perform any other required checks:

    ./gradlew check

Edit `$HOME/.gradle/gradle.properties` and add your GPG key information:

    signing.keyId=01234567                          # Your GPG key ID, as 8 hex digits
    signing.secretKeyRingFile=/path/to/secring.gpg  # Normally in $HOME/.gnupg/secring.gpg
    signing.password=YourSuperSecretPassphrase      # Plaintext passphrase to decrypt key
    nexusUsername=yourname                          # Your username on Apache's LDAP
    nexusPassword=password                          # Your password on Apache's LDAP

Putting your passwords there in plaintext is unfortunately unavoidable. The
[nexus plugin](https://github.com/bmuschko/gradle-nexus-plugin) supports asking
for them interactively, but unfortunately there's a
[Gradle issue](http://issues.gradle.org/browse/GRADLE-2357) which prevents us
from reading keyboard input (because we need `org.gradle.jvmargs` set).

To release to a local Maven repository:

    ./gradlew install

You should be able to see all the installed artifacts in the local repository now:

    find ~/.m2/repository/org/apache/datafu/

To build a tarball suitable for an ASF source release (and its accompanying MD5 file):

First, clean any non-checked-in files from git (this removes all such files without prompting):

    git clean -fdx

Alternatively, you can make a fresh clone of the repository to a separate directory:

    git clone https://git-wip-us.apache.org/repos/asf/incubator-datafu.git datafu-release
    cd datafu-release

To build a signed tarball:

    ./gradlew clean signSourceRelease

Alternatively, to build the tarball without the signatures:

    ./gradlew clean sourceRelease

The tarball can also be signed manually:

    gpg --sign --armor --detach-sig build/distribution/source/datafu-sources-*.tgz

