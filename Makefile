MODULE_NAME=Public.Protocols.MQTT
MODULE_LICENSE=GPL/LGPL/MPL
MODULE_DIR_NAME=Public_Protocols_MQTT

release: 
	hg tag -r tip RELEASE_1.${MIN}
	hg push
	hg archive -r RELEASE_1.${MIN} ${MODULE_DIR_NAME}-1.${MIN}
	cd ${MODULE_DIR_NAME}-1.${MIN} && \
	tar cvf ${MODULE_DIR_NAME}-1.${MIN}.tar ${MODULE_DIR_NAME}-1.${MIN}
	gzip ${MODULE_DIR_NAME}-1.${MIN}.tar
	rm -rf ${MODULE_DIR_NAME}-1.${MIN}
	pike upload_module_version.pike ${MODULE_NAME} 1.${MIN} "${MODULE_LICENSE}"
