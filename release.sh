#!/usr/bin/env sh

set -e

echo "$GPG_SECRET_KEYS" | base64 --decode | gpg --import --no-tty --batch --yes

GPG_KEY_ID=$(gpg --list-keys --with-colons  | awk -F: '/^pub:/ { print $5 }' | grep -E '.{8}$')
export GPG_KEY_ID

echo use-agent >> ~/.gnupg/gpg.conf
echo pinentry-mode loopback >> ~/.gnupg/gpg.conf
echo allow-loopback-pinentry >> ~/.gnupg/gpg-agent.conf
echo RELOADAGENT | gpg-connect-agent

export ORG_GRADLE_PROJECT_ossrhUsername="${SONATYPE_USERNAME}"
export ORG_GRADLE_PROJECT_ossrhPassword="${SONATYPE_PASSWORD}"

./gradlew -Psigning.gnupg.executable=gpg \
          -Psigning.gnupg.useLegacyGpg=true \
          -Psigning.gnupg.keyName="${GPG_KEY_ID}" \
          -Psigning.gnupg.passphrase="${GPG_PASSPHRASE}" \
      clean publish -x integrationTest --stacktrace