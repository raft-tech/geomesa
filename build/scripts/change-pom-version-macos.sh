# Update the from / to version labels
FROM_VERSION=5.4.0-SNAPSHOT
TO_VERSION=5.4.0-raft
find ../.. -name "pom.xml" -exec sed -i '' "s|<version>${FROM_VERSION}</version>|<version>${TO_VERSION}</version>|g" {} \;