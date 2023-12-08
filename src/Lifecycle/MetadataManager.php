<?php

namespace Thunk\Verbs\Lifecycle;

use Illuminate\Support\Collection;
use Thunk\Verbs\Event;
use Thunk\Verbs\Metadata;
use UnexpectedValueException;
use WeakMap;

class MetadataManager
{
    protected WeakMap $ephemeral;
	
	protected WeakMap $persistent;
	
	/** @var callable[] */
	protected array $callbacks = [];

    public function __construct()
    {
        $this->ephemeral = new WeakMap();
	    $this->persistent = new WeakMap();
    }
	
	public function createMetadataUsing(?callable $callback = null): void
	{
		if (is_null($callback)) {
			$this->callbacks = [];
		} else {
			$this->callbacks[] = $callback;
		}
	}

    public function setLastResults(Event $event, Collection $results): static
    {
        return $this->setEphemeral($event, '_last_results', $results);
    }

    public function getLastResults(Event $event): Collection
    {
        return $this->getEphemeral($event, '_last_results', new Collection());
    }

    public function getEphemeral(Event $event, ?string $key = null, mixed $default = null): mixed
    {
        return data_get($this->ephemeral[$event] ?? [], $key, $default);
    }

    public function setEphemeral(Event $event, string $key, mixed $value): static
    {
        $this->ephemeral[$event] ??= [];
        $this->ephemeral[$event][$key] = $value;

        return $this;
    }
	
	public function get(Event $event, ?string $key = null, mixed $default = null): mixed
	{
		$this->initialize($event);
		
		return data_get($this->persistent[$event], $key, $default);
	}
	
	public function set(Event $event, Metadata $metadata): static
	{
		$this->persistent[$event] = $metadata;
		
		return $this;
	}
	
	public function initialize(Event $event): Metadata
	{
		return $this->persistent[$event] ??= $this->makeMetadata($event);
	}
	
	protected function makeMetadata(Event $event): Metadata
	{
		$metadata = new Metadata();
		
		foreach ($this->callbacks as $callback) {
			$result = $callback($metadata, $event);
			
			$metadata = match (true) {
				$result instanceof Metadata => $result,
				is_iterable($result) => $metadata->merge($result),
				is_null($result) => $metadata,
				default => throw new UnexpectedValueException('Unexpected value returned from metadata callback.'),
			};
		}
		
		return $metadata;
	}
}
