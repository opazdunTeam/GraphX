"""Infrastructure failures that affect transport acknowledgement decisions."""


class RuntimeInfrastructureUncertainError(RuntimeError):
    """An external side effect may exist but cannot yet be authoritatively observed."""


class RuntimeFatalError(RuntimeError):
    """The consumer must stop after requeueing the current delivery."""


class RuntimeCapacityError(RuntimeFatalError):
    """A bounded runtime resource is exhausted and the consumer must stop."""


class RuntimeInfrastructureCapacityError(
    RuntimeInfrastructureUncertainError,
    RuntimeCapacityError,
):
    """Infrastructure outcome is uncertain because operation capacity is exhausted."""


class RuntimeInfrastructureFatalError(
    RuntimeInfrastructureUncertainError,
    RuntimeFatalError,
):
    """Infrastructure configuration or availability requires worker intervention."""
