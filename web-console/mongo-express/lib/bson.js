import parser, { toJSString } from 'mongodb-query-parser';
import { BSON, ObjectId } from 'mongodb';

const { EJSON } = BSON;

export const toBSON = parser;

// This function as the name suggests attempts to parse
// the free form string in to BSON, since the possibilities of failure
// are higher, this function uses a try..catch
export const toSafeBSON = function (string) {
  try {
    return toBSON(string);
  } catch (error) {
    console.error(error);
    return null;
  }
};

export const parseObjectId = function (string) {
  if (/^[\da-f]{24}$/i.test(string)) {
    return new ObjectId(string);
  }
  return toBSON(string);
};

// Convert BSON documents to string
export const toString = function (doc) {
  return toJSString(doc, '    ');
};

export const toJsonString = function (doc) {
  return EJSON.stringify(EJSON.serialize(doc));
};
