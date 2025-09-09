# Update the from / to version labels
FROM_VERSION=5.3.0
TO_VERSION=5.3.1-raft
find ../.. -name "pom.xml" -exec sed -i '' "s|<version>${FROM_VERSION}</version>|<version>${TO_VERSION}</version>|g" {} \;