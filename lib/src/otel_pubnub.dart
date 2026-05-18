// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_pubnub_suppression.dart';

const _tracerName = 'dartastic_otel_libs_pubnub';
const _systemValue = 'pubnub';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

/// Pair returned from a `tracedPubnub*` invoke callback.
class PubnubCallResult<T> {
  const PubnubCallResult({required this.value, this.timetoken});
  final T value;
  final String? timetoken;
}

/// Wraps a PubNub publish in a PRODUCER span using OTel messaging
/// semconv. PubNub assigns a `timetoken` to each accepted message,
/// recorded as `messaging.message.id`.
Future<T> tracedPubnubPublish<T>({
  required String channel,
  required Future<PubnubCallResult<T>> Function() invoke,
  int? payloadSize,
  String? uuid,
}) async {
  if (pubnubInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }

  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'publish',
    'messaging.destination.name': channel,
  };
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;
  if (uuid != null) m['pubnub.uuid'] = uuid;

  final span = _tracer().startSpan(
    'publish $channel',
    kind: SpanKind.producer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.timetoken != null) {
      span.addAttributes(OTel.attributes([
        OTel.attributeString('messaging.message.id', r.timetoken!),
        OTel.attributeString('pubnub.timetoken', r.timetoken!),
      ]));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(
      OTel.attributes([
        OTel.attributeString(
          ErrorResource.errorType.key,
          e.runtimeType.toString(),
        ),
      ]),
    );
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps a PubNub subscribe in a CLIENT span.
Future<T> tracedPubnubSubscribe<T>({
  required String channel,
  required Future<T> Function() invoke,
  bool? withPresence,
}) async {
  if (pubnubInstrumentationSuppressed()) {
    return invoke();
  }

  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'subscribe',
    'messaging.destination.name': channel,
    if (withPresence != null) 'pubnub.with_presence': withPresence,
  };

  final span = _tracer().startSpan(
    'subscribe $channel',
    kind: SpanKind.client,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(
      OTel.attributes([
        OTel.attributeString(
          ErrorResource.errorType.key,
          e.runtimeType.toString(),
        ),
      ]),
    );
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
