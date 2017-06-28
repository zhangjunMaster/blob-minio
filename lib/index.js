/**
 * Created by x on 17-6-20.
 */

'use strict';
var Minio = require('minio');
var fs = require('fs');
var util = require('util');

// Instantiate the minio client with the endpoint
// and access keys as shown below.

function Blob(param) {
    Minio.Client.call(this, param);
    this.baseUrl = 'http://' + param.endPoint + ':' + param.port
}

util.inherits(Blob, Minio.Client);

/**
 * param: buckName
 * return promise
 */

Blob.prototype.blobBucketExists = function (bucketName) {
    var self = this;
    return new Promise(function (resolve, reject) {
        self.bucketExists(bucketName, function (err) {
            if (err) {
                if (err.code == 'NoSuchBucket') return resolve(false);
                return reject(err)
            }
            resolve(true);
        })
    });
};

Blob.prototype.blobMakeBucket = function (bucketName, region) {
    if (!region) region = 'us-east-1';
    var self = this;
    return new Promise(function (resolve, reject) {
        self.bucketExists(bucketName, function (err) {
            if (!err) return resolve(true);
            if (err && err.code !== 'NoSuchBucket') return reject(err);
            self.makeBucket(bucketName, region, function (err) {
                if (err) return reject(err)
                resolve(true)
            })
        })
    });
};

Blob.prototype.blobRemoveBucket = function (bucketName) {
    var self = this;
    return new Promise(function (resolve, reject) {
        self.removeBucket(bucketName, function (err) {
            if (err) return reject(err);
            resolve(true);
        })
    });
}

/**
 * 只返回一个object
 * { name: '{.gif',
     lastModified: Tue Jun 20 2017 11:11:21 GMT+0800 (CST),
     etag: '1a28171164d3a633bce4bb2725bda792',
     size: 427034
     }
 */

Blob.prototype.blobListObjects = function (bucketName, prefix, recursive) {
    var self = this;
    return new Promise(function (resolve, reject) {
        var stream = self.listObjects(bucketName, '', true);
        stream.on('data', function (obj) {
            return resolve(obj)
        });
        stream.on('error', function (err) {
            return reject(err)
        })
    })
}

Blob.prototype.blobGetObject = function (bucketName, objectName) {
    var self = this;
    var chunks = [];
    return new Promise(function (resolve, reject) {
        var size = 0;
        self.getObject(bucketName, objectName, function (err, dataStream) {
            if (err) {
                return reject(err);
            }
            dataStream.on('data', function (chunk) {
                size += chunk.length
                chunks.push(chunk);
            });
            dataStream.on('end', function () {
                var buf = Buffer.concat(chunks, size);
                return resolve(buf)
            });
            dataStream.on('error', function (err) {
                return reject(err);
            })
        })
    })
};

/**
 * 下载文件到一个filepath
 */

Blob.prototype.blobFGetObject = function (bucketName, objectName, filePath) {
    var self = this;
    return new Promise(function (resolve, reject) {
        self.fGetObject(bucketName, objectName, filePath, function (err) {
            if (err) {
                return reject(err)
            }
            resolve(true)
        })
    })
};

Blob.prototype.blobPutFileObject = function (bucketName, objectName, filePath) {
    var self = this;
    return new Promise(function (resolve, reject) {

        var fileStream = fs.createReadStream(filePath);
        fs.stat(filePath, function (err, stats) {
            if (err) {
                return reject(err)
            }
            self.putObject(bucketName, objectName, fileStream, stats.size, function (err, etag) {
                if (err) {
                    return reject(err);
                }
                return resolve(self.baseUrl + '/' + bucketName + '/' + objectName);
            })
        });
    })
};

Blob.prototype.blobPutObject = function (bucketName, objectName, bufferOrString) {
    var self = this;
    return new Promise(function (resolve, reject) {
        self.putObject(bucketName, objectName, bufferOrString, function (err, etag) {
            if (err) return reject(err);
            return resolve(self.baseUrl + '/' + bucketName + '/' + objectName);
        })
    })
};

Blob.prototype.blobRemoveObject = function (bucketName, objectName) {
    var self = this;
    return new Promise(function (resolve, reject) {
        self.removeObject(bucketName, objectName, function (err) {
            if (err) {
                return reject(err)
            }
            resolve(true)
        })
    })
}
/**
 * 生成一个临时下载地址,有效期是7天,带有秘钥,这个不能存在数据库中,只能存储bucket和picture名称
 * 'http://127.0.0.1:9000/pic/123456.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=472AEXRNIHTL9I20JM5O%2F20170622%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20170622T094706Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=4cc61d606ea7fc5e761b7b562bff2380b36bc4d27f4cacfe0b88a489de9b101f'
 * 默认是30d
 */

Blob.prototype.blobPresignedGetObject = function (bucketName, objectName, expiry) {
    var self = this;
    if (!expiry) expiry = 7 * 24 * 60 * 60;
    return new Promise(function (resolve, reject) {
        self.presignedGetObject(bucketName, objectName, expiry, function (err, presignedUrl) {
            if (err) {
                return reject(err)
            }
            resolve(presignedUrl)
        })
    })
};

Blob.prototype.blobGetObjectUrl = function (bucketName, objectName) {
    var self = this;
    return new Promise(function (resolve, reject) {
        resolve(self.baseUrl + '/' + bucketName + '/' + objectName);
    })
}
/**
 * 获取bucket内的权限
 */

Blob.prototype.blobGetBucketPolicy = function (bucketName, objectPrefix) {
    var self = this;
    return new Promise(function (resolve, reject) {
        self.getBucketPolicy(bucketName, objectPrefix, function (err, policy) {
            if (err) return reject(err);
            resolve(policy)
        })
    })
};
/**
 * minio.Policy.NONE,minio.Policy.READONLY,minio.Policy.WRITEONLY,minio.Policy.READWRITE
 * 默认设置成可读和可写的
 */

Blob.prototype.blobSetBucketPolicy = function (bucketName, objectPrefix, bucketPolicy) {
    var self = this;
    if (!bucketPolicy) bucketPolicy = Minio.Policy.READWRITE;
    if (!objectPrefix) objectPrefix = '';
    return new Promise(function (resolve, reject) {
        self.setBucketPolicy(bucketName, objectPrefix, bucketPolicy, function (err) {
            if (err) reject(err);
            resolve(true)
        })
    })
};

Blob.prototype.blobStatObject = function (bucketName, objectName) {
    var self = this;
    return new Promise(function (resolve, reject) {
        self.statObject(bucketName, objectName, function (err, stat) {
            if (err) {
                if (err) reject(err);
            }
            resolve(stat);
        })
    })
}
module.exports = Blob;
