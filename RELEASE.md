# Apache DataFu Release Guide

This will guide you through the source release process for Apache DataFu.

For general information on the Apache Incubator release process, see the [Release Management](http://incubator.apache.org/guides/releasemanagement.html) page.

## Prerequisites

If this is your first time doing an Apache release, then there is some initial setup involved.  To perform a release, you will need to be able to sign the source tarball.  See the [Signing Releases](http://www.apache.org/dev/release-signing.html) page for information on how to do this.  You should read this page before proceeding.   In a nutshell, you'll need to follow the instructions at [How To OpenPGP](http://www.apache.org/dev/openpgp.html#generate-key) to generate a new code signing key and publish the public key in various places.

Once you have followed these instructions, you should have:

* Your public key uploaded to a public keyserver using the `gpg --send-keys` command
* Your public key listed in the `KEYS` file in this repo
* Your public key viewable at https://people.apache.org/keys/committer/your-alias.asc
* Your public key also viewable at https://people.apache.org/keys/group/datafu.asc

After completing this, you should also configure git to use your key for signing.  If your signing key is identified by `01234567`, then you can configure git with:

    git config --global user.signingkey 01234567

If you are using gpg2 then you'll need to tell git to use it.

    git config --global gpg.program gpg2

Tip: You may get an error about a passphrase not being provided when signing with git.  If this happens try running the command below, which should case the passphrase prompt to show in the terminal.

    export GPG_TTY=`tty`

## Code Validation

Before releasing, we must run various checks to ensure that files have the proper license headers and that all automated tests pass.  These checks can be run with:

    ./gradlew check

If this builds successfully then it means the tests pass and the report was successfully generated.  But, it doesn't mean that all license headers are in place.  You should open the report at `build/rat/rat-report.html` to validate that all files that are in scope (i.e. not excluded) have the appopriate headers.  Use the `rat` task to generate this report without running tests.  See `HEADER` for the contents of the license header.  These contents should appear at the top of the file as a comment.  If a file or set of files needs to be excluded from Rat validation, you can update the Rat exclusion list in `build.gradle`.

## Create a branch for release

Assuming you have are preparing to release version `x.y.z` from the current commit, then create a branch with:

        git checkout -b x.y.z
        git push origin x.y.z

## Create a Source Release

The following steps will build a tarball suitable for an ASF source release.  This also generates accompanying MD5 and ASC files.

First, clean any files unknown to git (WARNING: this removes all untracked files, including those listed in .gitignore, without prompting):

    git clean -fdx

Alternatively, you can make a fresh clone of the repository to a separate directory:

    git clone https://git-wip-us.apache.org/repos/asf/incubator-datafu.git incubator-datafu-release
    cd incubator-datafu-release

The source tarball needs to be signed.  You can do this either manually or automatically.  To have it signed automatically you'll need to perform a one-time configuration step.  Edit `$HOME/.gradle/gradle.properties` and add your GPG key information:

    signing.keyId=01234567                          # Your GPG key ID, as 8 hex digits
    signing.secretKeyRingFile=/path/to/secring.gpg  # Normally in $HOME/.gnupg/secring.gpg
    signing.password=YourSuperSecretPassphrase      # Plaintext passphrase to decrypt key

The GPG key ID can be found by running `gpg --list-keys`.

To generate the source release, run:

    ./gradlew clean release -Prelease=true

This generates a source tarball.  The `-Prelease=true` setting prevents `-SNAPSHOT` from being appended to the version,
which is the default behavior.  It also prevents any builds from the extracted source tarball from including `-SNAPSHOT` in the version.  It achieves this by modifying `gradle.properties` within the generated archive.

If you have configured your key information in your `gradle.properties` then you the archive should automatically be signed.  There should now be a corresponding ASC file alongside the tarball and MD5 file.  Otherwise you'll need to sign it manually by running:

    gpg --sign --armor --detach-sig build/distribution/source/apache-datafu-sources-*.tgz

If you have GPG v2 installed then you'll need to use `gpg2` instead.

## Upload the Source Release

You should make the release candidate available in [https://dist.apache.org/repos/dist/dev/incubator/datafu](https://dist.apache.org/repos/dist/dev/incubator/datafu).  For example, if you are releasing release candidate RC0 for version `x.y.z` then you should upload the source distribution files to:

    https://dist.apache.org/repos/dist/dev/incubator/datafu/incubator-datafu-x.y.z-rc0/

To create a release folder and check out out (be sure to substitute x.y.z for the actual version):

    svn mkdir https://dist.apache.org/repos/dist/dev/incubator/datafu/apache-datafu-incubating-x.y.z-rc0
    svn co https://dist.apache.org/repos/dist/dev/incubator/datafu/apache-datafu-incubating-x.y.z-rc0 apache-datafu-incubating-x.y.z-rc0
    cd apache-datafu-incubating-x.y.z-rc0

You could then add the source release as described above and commit.

## Tag the release

You should tag the release candidate in git.  Assuming you are releasing release candidate RC0 for version `x.y.z` then you can attach a tag to the current commit with:

    git tag -s release-x.y.z-rc0 -m 'Apache DataFu (incubating) x.y.z RC0'

Then push the tag:

    git push origin release-x.y.z-rc0

## Staging artifacts in Maven

General information on publishing to Maven can be found [here](http://www.apache.org/dev/publishing-maven-artifacts.html).  To upload the archive to the Apache Nexus staging repository, run:

    ./gradlew uploadArchives -Prelease=true -PnexusUsername=yourNexusUsername -PnexusPassword=yourNexusPassword -Psigning.keyId=yourGpgKeyId -Psigning.password=yourGpgSigningPassword

Note that if you are running the above command from a source release you don't need `-Prelease=true`.

If you now visit the [Apache Nexus Repository](https://repository.apache.org) and click on Staging Repositories, you should see a repository named orgapachedatafu-xxxx, where xxxx is some number.  Select the repository and browse the content to make sure the set of files looks right.  If it looks correct then Close the repository.  The repository is now ready for testing.  If you look at the summary there is a URL for the repository that may be used to fetch the archives.

Let's suppose you have a Gradle project you'd like to use to test DataFu.  You can add the URL for the Staging Repository to your `build.gradle` like this:

    repositories {
      mavenCentral()
      maven {
        url 'https://repository.apache.org/content/repositories/orgapachedatafu-xxxx'
      }
    }

You can now depend on the versions of the archives in this Staging Repository in your `build.gradle`:

    dependencies {
      compile "org.apache.datafu:datafu-pig-incubating:x.y.z"
      compile "org.apache.datafu:datafu-hourglass-incubating:x.y.z"
    }

You could also visit the Staging Repository URL in your browser and download the files for testing.

## Call for a vote to release

At this point you should have:

1. Published a source release for testing
2. Staged artifacts in Nexus built from that source archive for testing

Now you can call a vote in the DataFu dev mailing list for release.   Look in the archives at previous votes for an example.  If the vote passes then you may call a vote in the Incubator general mailing list (this is necessary because DataFu is still incubating).

## Testing the source release

Once you have built the source tarball, you should verify that it can be used.  Follow the instructions in the `README.md` file assuming you are someone who has just downloaded the source tarball and want to use it.

### Releasing to your local Maven repository

You may want to release binaries to your local Maven repository under your home directory to do local testing against it.  To do so, run:

    ./gradlew install -Prelease=true

You should be able to see all the installed artifacts in the local repository now:

    find ~/.m2/repository/org/apache/datafu/

Again, setting `release=true` prevents `-SNAPSHOT` from being appended to the version.

## Publishing the release

Once the Incubator general vote has passed, you can publish the source release and artifacts.

### Source release

The DataFu source release are checked into SVN under [https://dist.apache.org/repos/dist/release/incubator/datafu](https://dist.apache.org/repos/dist/release/incubator/datafu).

To see all the previous releases:

    svn list https://dist.apache.org/repos/dist/release/incubator/datafu

Create a directory for the release (replace `x.y.z` with the release number):

    svn mkdir https://dist.apache.org/repos/dist/release/incubator/datafu/apache-datafu-incubating-x.y.z
    svn co https://dist.apache.org/repos/dist/release/incubator/datafu/apache-datafu-incubating-x.y.z apache-datafu-incubating-x.y.z-release
    cd apache-datafu-incubating-x.y.z-release

Now copy the source release files into this directory and commit them.  Within 24 hours they will be distributed to the mirrors.  Then it should be available for download at `http://www.apache.org/dyn/closer.cgi/incubator/datafu/apache-datafu-incubating-x.y.z/`.

### Artifacts

To distribute the artifacts, simple select the staged repository for DataFu that you prepared in Nexus and chooose Release.  They should then be available within the next day or so in the [central repository](http://search.maven.org/).