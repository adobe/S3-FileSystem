# Copyright 2022 Adobe. All rights reserved.
# This file is licensed to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License. You may obtain a copy
# of the License at http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
# OF ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

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
      clean publish --stacktrace