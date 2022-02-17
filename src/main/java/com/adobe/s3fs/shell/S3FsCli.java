/*
Copyright 2021 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

package com.adobe.s3fs.shell;

import com.adobe.s3fs.shell.commands.fsck.FsckCommandLoader;
import com.adobe.s3fs.shell.commands.fsck.FullRestore;
import com.adobe.s3fs.shell.commands.fsck.Verify;
import com.adobe.s3fs.shell.commands.fsck.S3Ls;
import com.adobe.s3fs.shell.commands.tools.*;
import com.adobe.s3fs.shell.helpers.CommaSeparatedRestrictionFactory;
import com.adobe.s3fs.shell.helpers.CommaSeparatedValues;
import com.adobe.s3fs.shell.helpers.CustomOptionsParser;
import com.github.rvesse.airline.annotations.Cli;
import com.github.rvesse.airline.annotations.Group;
import com.github.rvesse.airline.annotations.Parser;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;
import com.github.rvesse.airline.restrictions.factories.RestrictionRegistry;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static com.adobe.s3fs.shell.CommandGroups.FSCK;
import static com.adobe.s3fs.shell.CommandGroups.TOOLS;

@Cli(
    name = "s3fs",
    description = "s3fs cli",
    parserConfiguration =
        @Parser(
            defaultParsersFirst = false,
            optionParsers = {CustomOptionsParser.class}),
    groups = {
      @Group(
          name = FSCK,
          description = "file system check command group",
          commands = {
            FullRestore.class,
            Verify.class,
            S3Ls.class,
            FsckCommandLoader.class
          }),
      @Group(
          name = TOOLS,
          description = "tools command group",
          commands = {MetaStoreReader.class,
              OperationLogReader.class,
              RawS3BucketSize.class,
              DynamoDBStreamLister.class,
              PurgeMetadata.class,
              PurgeBucket.class
          })
    },
    commands = {Help.class})
public class S3FsCli {
  private static final Logger LOG = LoggerFactory.getLogger(S3FsCli.class);

  static {
    RestrictionRegistry.addOptionRestriction(CommaSeparatedValues.class, new CommaSeparatedRestrictionFactory() );
  }

  public static void main(String[] args) throws IOException {
    try (InputStream is = ClassLoader.getSystemResourceAsStream("conf/s3fs_cli.log4j.properties")) {
      PropertyConfigurator.configure(is);
    }
    com.github.rvesse.airline.Cli<Runnable> cli =
        new com.github.rvesse.airline.Cli<>(S3FsCli.class);
    CliHelper cliHelper = new CliHelper();
    Optional<? extends Runnable> cmd = cliHelper.parseCli(cli, args);
    if (!cmd.isPresent()) {
      LOG.info("Parsing failed. Early exit.");
      System.exit(CommandStatus.FAILED.getStatus());
    }
    CommandStatus cmdStatus = cmd.map(cliHelper::executeCmd).get(); // NOSONAR
    if (LOG.isDebugEnabled()) {
      LOG.debug("Exiting with Code {}", cmdStatus.getStatus());
    }
    System.exit(cmdStatus.getStatus());
  }

  private enum CommandStatus {
    SUCCESSFUL(0),
    FAILED(1);

    private final int status;

    CommandStatus(int status) {
      this.status = status;
    }

    public int getStatus() {
      return status;
    }
  }

  private static class CliHelper {
    private <T extends Runnable> Optional<T> parseCli(
        com.github.rvesse.airline.Cli<T> cli, String[] args) {
      try {
        return Optional.ofNullable(cli.parse(args));
      } catch (ParseException pe) {
        LOG.error("Parse error: ", pe);
      } catch (Exception e) {
        LOG.error("Unexpected error: ", e);
      }
      return Optional.empty();
    }

    /**
     *
     * @param cmd
     * @param <T>
     * @return CommandStatus enum which holds the status of running cmd
     */
    private <T extends Runnable> CommandStatus executeCmd(T cmd) {
      try {
        cmd.run();
        return CommandStatus.SUCCESSFUL;
      } catch (Exception e) {
        LOG.error("Command threw error: ", e);
      }
      return CommandStatus.FAILED;
    }
  }
}
