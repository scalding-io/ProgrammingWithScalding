        <!-- Add in properties -->
        <typesafe.config.version>1.0.0</typesafe.config.version>

        <!-- Add in dependencies section -->
        <dependency>
            <groupId>com.typesafe</groupId>
            <artifactId>config</artifactId>
            <version>${typesafe.config.version}</version>
        </dependency>

        <!-- Add in build -->
        <plugin>
			<groupId>org.apache.maven.plugins</groupId>
			<artifactId>maven-dependency-plugin</artifactId>
			<executions>
				<execution>
					<id>copy-dependencies</id>
					<phase>install</phase>
					<goals>
						<goal>copy-dependencies</goal>
					</goals>
					<configuration>
						<outputDirectory>./job-libraries</outputDirectory>
                        <includeScope>runtime</includeScope>
                        <overWriteReleases>false</overWriteReleases>
						<overWriteSnapshots>false</overWriteSnapshots>
						<overWriteIfNewer>true</overWriteIfNewer>
					</configuration>
				</execution>
			</executions>
		</plugin>