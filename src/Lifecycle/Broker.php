<?php

namespace Thunk\Verbs\Lifecycle;

use Illuminate\Contracts\Cache\Lock;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Thunk\Verbs\CommitsImmediately;
use Thunk\Verbs\Contracts\BrokersEvents;
use Thunk\Verbs\Contracts\StoresEvents;
use Thunk\Verbs\Event;
use Thunk\Verbs\Exceptions\ConcurrencyException;
use Thunk\Verbs\Lifecycle\Queue as EventQueue;

class Broker implements BrokersEvents
{
    use BrokerConvenienceMethods;

    public bool $commit_immediately = false;

    public function __construct(
        protected Dispatcher $dispatcher,
        protected MetadataManager $metadata,
    ) {
    }

    public function fire(Event $event): ?Event
    {
        if ($this->is_replaying) {
            return null;
        }

        // NOTE: Any changes to how the dispatcher is called here
        // should also be applied to the `replay` method

        $states = $event->states();

        $states->each(fn ($state) => Guards::for($event, $state)->check());

        Guards::for($event, null)->check();

        $states->each(fn ($state) => $this->dispatcher->apply($event, $state));

        app(Queue::class)->queue($event);

        $this->dispatcher->fired($event, $states);

        if ($this->commit_immediately || $event instanceof CommitsImmediately) {
            $this->commit();
        }

        return $event;
    }

    public function commit(): bool
    {
        /** @var StateManager $stateManager */
        $stateManager = app(StateManager::class);
        $loadedStates = $stateManager->loaded();

        /** @var Lock[] $locks */
        $locks = collect($loadedStates)
            ->map(fn (string $key) => \Cache::lock('verbs_state_lock_' . $key, 60))
            ->all();

        $acquiredLocks = [];

        try {
            foreach ($locks as $lock) {
                if (!$lock->get()) {
                    throw new ConcurrencyException("Unable to acquire lock for all keys.");
                }
                $acquiredLocks[] = $lock; // Add lock to the list of acquired locks
            }

            $events = app(EventQueue::class)->flush();

            if (empty($events)) {
                return true;
            }

            // FIXME: Only write changes + handle aggregate versioning
            app(StateManager::class)->writeSnapshots();
        } catch (ConcurrencyException $e) {
            foreach ($acquiredLocks as $lock) {
                $lock->release(); // Release only the locks that were successfully acquired
            }
            throw $e;
        }
        finally {
            foreach ($acquiredLocks as $lock) {
                $lock->release(); // Release only the locks that were successfully acquired
            }
        }


        foreach ($events as $event) {
            $this->metadata->setLastResults($event, $this->dispatcher->handle($event, $event->states()));
        }

        return $this->commit();
    }

    public function replay(?callable $beforeEach = null, ?callable $afterEach = null)
    {
        $this->is_replaying = true;

        try {
            app(StateManager::class)->reset(include_storage: true);

            app(StoresEvents::class)->read()
                ->each(function (Event $event) use ($beforeEach, $afterEach) {
                    app(StateManager::class)->setReplaying(true);

                    if ($beforeEach) {
                        $beforeEach($event);
                    }

                    $event->states()->each(fn ($state) => $this->dispatcher->apply($event, $state));
                    $this->dispatcher->replay($event, $event->states());

                    if ($afterEach) {
                        $afterEach($event);
                    }

                    return $event;
                });
        } finally {
            app(StateManager::class)->setReplaying(false);
            $this->is_replaying = false;
        }
    }

    public function commitImmediately(bool $commit_immediately = true): void
    {
        $this->commit_immediately = $commit_immediately;
    }
}
