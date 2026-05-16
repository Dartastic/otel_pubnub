// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_pubnub/otel_pubnub.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_pubnub-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('publish records PRODUCER span with messaging.* attrs', () async {
    await tracedPubnubPublish<String>(
      channel: 'lobby',
      payloadSize: 64,
      uuid: 'user-u',
      invoke: () async => const PubnubCallResult(
        value: 'ok',
        timetoken: '17050000000000000',
      ),
    );
    final span = spans.findSpanByName('publish lobby');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.producer);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'pubnub');
    expect(a['messaging.operation'], 'publish');
    expect(a['messaging.destination.name'], 'lobby');
    expect(a['messaging.message.body.size'], 64);
    expect(a['pubnub.uuid'], 'user-u');
    expect(a['messaging.message.id'], '17050000000000000');
    expect(a['pubnub.timetoken'], '17050000000000000');
  });

  test('subscribe records CLIENT span', () async {
    await tracedPubnubSubscribe<String>(
      channel: 'lobby',
      withPresence: true,
      invoke: () async => 'subscribed',
    );
    final span = spans.findSpanByName('subscribe lobby');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.client);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'pubnub');
    expect(a['messaging.operation'], 'subscribe');
    expect(a['messaging.destination.name'], 'lobby');
    expect(a['pubnub.with_presence'], true);
  });

  test('records exception on throw', () async {
    await expectLater(
      tracedPubnubPublish<String>(
        channel: 'c',
        invoke: () async => throw StateError('boom'),
      ),
      throwsA(isA<StateError>()),
    );
    expect(
      spans.findSpanByName('publish c')!.status,
      SpanStatusCode.Error,
    );
  });

  test('suppression skips spans', () async {
    await runWithoutPubnubInstrumentationAsync(() async {
      await tracedPubnubPublish<String>(
        channel: 'c',
        invoke: () async => const PubnubCallResult(value: 'v'),
      );
      await tracedPubnubSubscribe<String>(
        channel: 'c',
        invoke: () async => 'ok',
      );
    });
    expect(spans.findSpansStartingWith('publish '), isEmpty);
    expect(spans.findSpansStartingWith('subscribe '), isEmpty);
  });
}
