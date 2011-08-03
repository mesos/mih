PKG_DIR = src/org/apache/mesos/mih
SOURCES = $(PKG_DIR)/MIH.java $(PKG_DIR)/Master.java $(PKG_DIR)/Slave.java
MANIFEST = src/Manifest.txt

mih.jar: $(SOURCES) $(MANIFEST) | classes
ifeq ($(HADOOP_HOME),)
	@echo "HADOOP_HOME needs to be set to compile MIH" >&2
	@exit 2
else
	javac -d classes -cp $(HADOOP_HOME)/hadoop-*-core.jar $(SOURCES)
	jar cfm mih.jar $(MANIFEST) -C classes org
endif

classes:
	mkdir -p classes

clean:
	rm -fr mih.jar classes
