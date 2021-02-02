/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.builtins.module;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.ui.ProgressBar;
import org.apache.ignite.cli.builtins.SystemPathResolver;
import org.apache.ivy.Ivy;
import org.apache.ivy.core.IvyContext;
import org.apache.ivy.core.event.EventManager;
import org.apache.ivy.core.event.download.EndArtifactDownloadEvent;
import org.apache.ivy.core.event.resolve.EndResolveDependencyEvent;
import org.apache.ivy.core.event.resolve.EndResolveEvent;
import org.apache.ivy.core.event.retrieve.EndRetrieveArtifactEvent;
import org.apache.ivy.core.module.descriptor.DefaultDependencyDescriptor;
import org.apache.ivy.core.module.descriptor.DefaultModuleDescriptor;
import org.apache.ivy.core.module.descriptor.ModuleDescriptor;
import org.apache.ivy.core.module.id.ModuleRevisionId;
import org.apache.ivy.core.report.ResolveReport;
import org.apache.ivy.core.resolve.ResolveOptions;
import org.apache.ivy.core.retrieve.RetrieveOptions;
import org.apache.ivy.core.retrieve.RetrieveReport;
import org.apache.ivy.core.settings.IvySettings;
import org.apache.ivy.plugins.resolver.ChainResolver;
import org.apache.ivy.plugins.resolver.IBiblioResolver;
import org.apache.ivy.util.AbstractMessageLogger;
import org.apache.ivy.util.Message;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolver of maven artifacts with Ivy.
 */
@Singleton
public class MavenArtifactResolver {
    /** Pattern for naming artifact files. **/
    private static final String FILE_ARTIFACT_PATTERN = "[artifact](-[classifier]).[revision].[ext]";

    /** Resolver of system paths. **/
    private final SystemPathResolver pathRslvr;

    /** System terminal instance, needed for receiving info about terminal settings. **/
    private final Terminal terminal;

    /** Console writer for output user messages. **/
    private PrintWriter out;

    /**
     * Creates resolver
     *
     * @param pathRslvr Resolver of system paths like home directory and etc.
     * @param terminal User system terminal.
     */
    @Inject
    public MavenArtifactResolver(SystemPathResolver pathRslvr, Terminal terminal) {
        this.pathRslvr = pathRslvr;
        this.terminal = terminal;
    }

    /**
     * Sets writer for user messages.
     *
     * @param out PrintWriter
     */
    public void setOut(PrintWriter out) {
        this.out = out;
    }

    /**
     * Downloads and copies artifact and its dependencies to {mavenRoot}.
     *
     * @param mavenRoot Path where artifacts will be copied to.
     * @param grpId Maven group id of the artifact.
     * @param artifactId Maven artifact id of the artifact.
     * @param ver Manve version of the artifact.
     * @param customRepositories Urls with custom maven repositories to resolve artifact.
     * @return Result of resolving with files' paths of resolved artifact + dependencies.
     * @throws IOException if connection issues occurred during resolving
     *                     or if r/w issues occurred during the retrieving of artifacts
     */
    public ResolveResult resolve(
        Path mavenRoot,
        String grpId,
        String artifactId,
        String ver,
        List<URL> customRepositories
    ) throws IOException {
        Ivy ivy = ivyInstance(customRepositories); // needed for init right output logger before any operations

        out.println("Installing " + String.join(":", grpId, artifactId, ver) + "...");

        try (ProgressBar bar = new ProgressBar(out, 100, terminal.getWidth())) {
            ivy.getEventManager().addIvyListener(event -> {
                if (event instanceof EndResolveEvent) {
                    int cnt = ((EndResolveEvent)event).getReport().getArtifacts().size();

                    bar.setMax(cnt * 3);
                }
                else if (event instanceof EndArtifactDownloadEvent ||
                    event instanceof EndResolveDependencyEvent ||
                    event instanceof EndRetrieveArtifactEvent)
                    bar.step();
            });

            ModuleDescriptor md = rootModuleDescriptor(grpId, artifactId, ver);

            // Step 1: you always need to resolve before you can retrieve
            //
            ResolveOptions ro = new ResolveOptions();
            // this seems to have no impact, if you resolve by module descriptor
            //
            // (in contrast to resolve by ModuleRevisionId)
            ro.setTransitive(true);
            // if set to false, nothing will be downloaded
            ro.setDownload(true);

            try {
                // now resolve
                ResolveReport rr = ivy.resolve(md, ro);

                if (rr.hasError())
                    throw new IgniteCLIException(rr.getAllProblemMessages().toString());

                // Step 2: retrieve
                ModuleDescriptor m = rr.getModuleDescriptor();

                RetrieveReport retrieveReport = ivy.retrieve(
                    m.getModuleRevisionId(),
                    new RetrieveOptions()
                        // this is from the envelop module
                        .setConfs(new String[] {"default"})
                        .setDestArtifactPattern(
                            mavenRoot.resolve("[artifact](-[classifier]).[revision].[ext]").toFile().getAbsolutePath())
                );

                return new ResolveResult(
                    retrieveReport.getRetrievedFiles().stream().map(File::toPath).collect(Collectors.toList())
                );
            }
            catch (ParseException e) {
                throw new IOException(e);
            }
        }
    }

    /**
     * Gets artifact file name by artifactId and version
     * <p>
     * Note: Current implementation doesn't support artifacts with classifiers or non-jar packaging
     *
     * @param artfactId Maven artifact id.
     * @param ver Maven version
     * @return File name
     */
    public static String fileNameByArtifactPattern(
        String artfactId,
        String ver) {
        return FILE_ARTIFACT_PATTERN
            .replace("[artifact]", artfactId)
            .replace("(-[classifier])", "")
            .replace("[revision]", ver)
            .replace("[ext]", "jar");
    }

    /**
     * Prepares Ivy instance for artifact resolving.
     *
     * @param repositories Additional maven repositories
     * @return Ivy instance
     */
    private Ivy ivyInstance(List<URL> repositories) {
        File tmpDir;

        try {
            tmpDir = Files.createTempDirectory("ignite-installer-cache").toFile();
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't create temp directory for ivy");
        }

        tmpDir.deleteOnExit();

        EventManager evtMgr = new EventManager();

        IvySettings ivySettings = new IvySettings();
        ivySettings.setDefaultCache(tmpDir);
        ivySettings.setDefaultCacheArtifactPattern(FILE_ARTIFACT_PATTERN);

        ChainResolver chainRslvr = new ChainResolver();
        chainRslvr.setName("chainResolver");
        chainRslvr.setEventManager(evtMgr);

        for (URL repoUrl : repositories) {
            IBiblioResolver br = new IBiblioResolver();
            br.setEventManager(evtMgr);
            br.setM2compatible(true);
            br.setUsepoms(true);
            br.setRoot(repoUrl.toString());
            br.setName(repoUrl.getPath());

            chainRslvr.add(br);
        }

        // use the biblio resolver, if you consider resolving
        // POM declared dependencies
        IBiblioResolver br = new IBiblioResolver();
        br.setEventManager(evtMgr);
        br.setM2compatible(true);
        br.setUsepoms(true);
        br.setName("central");

        chainRslvr.add(br);

        IBiblioResolver locBr = new IBiblioResolver();
        locBr.setEventManager(evtMgr);
        locBr.setM2compatible(true);
        locBr.setUsepoms(true);
        locBr.setRoot("file://" + pathRslvr.osHomeDirectoryPath().resolve(".m2").resolve("repository/"));
        locBr.setName("local");

        chainRslvr.add(locBr);

        ivySettings.addResolver(chainRslvr);
        ivySettings.setDefaultResolver(chainRslvr.getName());

        Ivy ivy = new Ivy();

        ivy.getLoggerEngine().setDefaultLogger(new IvyLogger());

        // needed for setting the message logger before logging info from loading settings
        IvyContext.getContext().setIvy(ivy);

        ivy.setSettings(ivySettings);

        ivy.bind();

        return ivy;
    }

    /**
     * Prepares Ivy module descriptor with target maven artifact as a dependency.
     * Then descriptor can be used for further resolving the artifact dependencies.
     * Existence of this descriptor is Ivy's requirement.
     *
     * @param grpId Maven group id.
     * @param artifactId Maven artifact id.
     * @param ver Maven artifact version.
     * @return Prepared for resolving module descriptor.
     */
    private ModuleDescriptor rootModuleDescriptor(String grpId, String artifactId, String ver) {
        // 1st create an ivy module (this always(!) has a "default" configuration already)
        DefaultModuleDescriptor md = DefaultModuleDescriptor.newDefaultInstance(
            // give it some related name (so it can be cached)
            ModuleRevisionId.newInstance(
                "org.apache.ignite",
                "installer-envelope",
                "working"
            )
        );

        // 2. add dependencies for what we are really looking for
        ModuleRevisionId ri = ModuleRevisionId.newInstance(
            grpId,
            artifactId,
            ver
        );

        // don't go transitive here, if you want the single artifact
        DefaultDependencyDescriptor dd = new DefaultDependencyDescriptor(md, ri, false, true, true);

        // map to master to just get the code jar. See generated ivy module xmls from maven repo
        // on how configurations are mapped into ivy. Or check
        // e.g. http://lightguard-jp.blogspot.de/2009/04/ivy-configurations-when-pulling-from.html
        dd.addDependencyConfiguration("default", "master");
        dd.addDependencyConfiguration("default", "runtime");
        dd.addDependencyConfiguration("default", "compile");

        md.addDependency(dd);

        return md;
    }

    /**
     * Ivy logger for routing all ivy logs to general logging system of CLI.
     */
    private static class IvyLogger extends AbstractMessageLogger {
        /** Common loogger */
        private final Logger log = LoggerFactory.getLogger(IvyLogger.class);

        /** {@inheritDoc} */
        @Override protected void doProgress() {
            // no-op
        }

        /** {@inheritDoc} */
        @Override protected void doEndProgress(String msg) {
            // no-op
        }

        /** {@inheritDoc} */
        @Override public void log(String msg, int level) {
            switch (level) {
                case Message.MSG_ERR:
                    log.error(msg);
                    break;

                case Message.MSG_WARN:
                    log.warn(msg);
                    break;

                case Message.MSG_INFO:
                    log.info(msg);
                    break;

                case Message.MSG_VERBOSE:
                    log.debug(msg);
                    break;

                case Message.MSG_DEBUG:
                    log.trace(msg);
                    break;
            }
        }

        /** {@inheritDoc} */
        @Override public void rawlog(String msg, int level) {
            log(msg, level);
        }
    }
}
