import pytest
from abc import ABC, abstractmethod
from dependency_injector import containers, providers

from cqrs.container.dependency_injector import DependencyInjectorCQRSContainer


class UserRepository:
    def get_user(self, user_id: int) -> str:
        return f"User {user_id}"


class IUserService(ABC):
    @abstractmethod
    def fetch_user(self, user_id: int) -> str:
        pass


class UserService(IUserService):
    def __init__(self, repository: UserRepository) -> None:
        self.repository = repository

    def fetch_user(self, user_id: int) -> str:
        return self.repository.get_user(user_id)


class OverriddenUserService(UserService):
    def fetch_user(self, user_id: int) -> str:
        return f"Overridden User {user_id}"


class INotificationService(ABC):
    @abstractmethod
    def send(self, message: str) -> str:
        pass


class EmailNotificationService(INotificationService):
    def send(self, message: str) -> str:
        return f"Email sent: {message}"


class SubdomainContainer(containers.DeclarativeContainer):
    notification_service = providers.Factory(EmailNotificationService)


class Container(containers.DeclarativeContainer):
    user_repository = providers.Factory(UserRepository)
    user_service = providers.Factory(
        UserService,
        repository=user_repository,
    )

    subdomain_container = providers.Container(SubdomainContainer)


class TestDependencyInjectorCQRSContainer:
    """
    Test suite for DependencyInjectorCQRSContainer adapter.

    This test suite validates the integration between dependency-injector containers
    and the CQRS framework, covering attachment, resolution strategies, error handling,
    and edge cases like provider overrides and container re-attachment.
    """

    async def test_attach_external_container(self) -> None:
        """
        Attaching external container stores reference and makes it accessible.

        Verifies that after calling attach_external_container(), the external
        container is properly stored and can be accessed via the external_container
        property. This is the foundational setup step required before any resolution.
        """
        cqrs_container = DependencyInjectorCQRSContainer()
        container = Container()

        cqrs_container.attach_external_container(container)

        assert cqrs_container.external_container is container

    async def test_resolve_exact_type_match(self) -> None:
        """
        Resolution succeeds for exact type match (Strategy 1).

        Tests the primary resolution strategy where the requested type matches
        exactly with a type registered in the container. This validates that:
        - The type-to-provider mapping was built correctly during attachment
        - The provider can be retrieved by its path
        - The provider creates functional instances
        """
        cqrs_container = DependencyInjectorCQRSContainer()
        container = Container()
        cqrs_container.attach_external_container(container)

        repository = await cqrs_container.resolve(UserRepository)

        assert isinstance(repository, UserRepository)
        assert repository.get_user(1) == "User 1"

    async def test_resolve_with_dependencies(self) -> None:
        """
        Resolution handles services with constructor-injected dependencies.

        Validates that the dependency-injector container correctly wires up
        dependencies when creating instances. The CQRS adapter delegates instance
        creation to the underlying dependency-injector container, which handles:
        - Resolving transitive dependencies (UserService → UserRepository)
        - Injecting dependencies into constructors
        - Managing the lifecycle of dependencies based on provider scope

        This test confirms the adapter doesn't interfere with the dependency
        injection process and properly invokes the providers.
        """
        cqrs_container = DependencyInjectorCQRSContainer()
        container = Container()
        cqrs_container.attach_external_container(container)

        service = await cqrs_container.resolve(UserService)

        assert isinstance(service, UserService)
        assert isinstance(service.repository, UserRepository)
        assert service.fetch_user(42) == "User 42"

    async def test_resolve_type_not_found(self) -> None:
        """
        Resolution raises ValueError for unregistered types.

        Validates error handling when attempting to resolve a type that:
        - Has no exact match in the type-to-provider mapping
        - Has no registered subclasses (inheritance match fails)

        This ensures the container fails fast with a clear error message rather
        than returning None or raising a cryptic AttributeError.
        """
        cqrs_container = DependencyInjectorCQRSContainer()
        container = Container()
        cqrs_container.attach_external_container(container)

        class UnregisteredService:
            pass

        with pytest.raises(ValueError) as exc_info:
            await cqrs_container.resolve(UnregisteredService)

        assert "Provider for type UnregisteredService not found" in str(exc_info.value)

    async def test_resolve_super_class(self) -> None:
        """
        Resolution finds concrete implementation via inheritance matching (Strategy 2).

        Tests the secondary resolution strategy that enables Dependency Inversion
        Principle by allowing handlers to depend on abstractions rather than concretions.

        When requesting IUserService (abstract interface):
        - Strategy 1 (exact match) fails: no provider registered for IUserService
        - Strategy 2 (inheritance match) succeeds: finds UserService which implements IUserService
        - Returns instance of the concrete implementation

        This is the key feature that enables programming to interfaces in CQRS handlers
        without explicitly registering every abstract interface in the container.
        """
        cqrs_container = DependencyInjectorCQRSContainer()
        container = Container()
        cqrs_container.attach_external_container(container)

        service = await cqrs_container.resolve(IUserService)
        assert isinstance(service, UserService)
        assert service.fetch_user(42) == "User 42"

    async def test_override_provider_with_limited_support(self) -> None:
        """
        Provider overrides work through inheritance matching, with known limitations.

        Tests the interaction between dependency-injector's provider override mechanism
        and the CQRS adapter's type resolution. This validates a common testing scenario
        where providers are overridden to inject mocks or alternative implementations.

        How It Works:
            1. Initial mapping: UserService → ("user_service",)
            2. Provider override: container.user_service now creates OverriddenUserService
            3. Resolution via IUserService:
               - No exact match for IUserService
               - Inheritance match finds UserService (still in mapping)
               - Path ("user_service",) now points to overridden provider
               - Overridden provider creates OverriddenUserService instance

        Known Limitation:
            The type mapping stores the original registered type (UserService), not the
            overridden type (OverriddenUserService). This means:
            - ✓ Resolving via abstract interface works (IUserService → UserService path → override)
            - ✗ Directly resolving OverriddenUserService fails (not in mapping)

        This limitation is acceptable because:
            - Encourages programming to interfaces (best practice)
            - Overrides are typically used in testing scenarios
            - Direct resolution of override types is an anti-pattern
        """
        cqrs_container = DependencyInjectorCQRSContainer()
        container = Container()
        cqrs_container.attach_external_container(container)

        container.user_service.override(
            providers.Factory(
                OverriddenUserService,
                repository=container.user_repository,
            )
        )

        # Works: Resolve via abstract interface (inheritance match + override)
        service = await cqrs_container.resolve(IUserService)
        assert isinstance(service, OverriddenUserService)
        assert service.fetch_user(42) == "Overridden User 42"

        # Limitation: Cannot resolve concrete overridden type directly
        with pytest.raises(ValueError):
            await cqrs_container.resolve(OverriddenUserService)

    async def test_external_container_not_attached(self) -> None:
        """
        Accessing external container before attachment raises clear error.

        Validates fail-fast behavior when attempting to access the external_container
        property before attach_external_container() has been called. This prevents
        confusing NoneType errors downstream and provides clear guidance.

        The error message explicitly states "External container not attached" to
        guide developers to the correct initialization sequence.
        """
        cqrs_container = DependencyInjectorCQRSContainer()

        with pytest.raises(ValueError) as exc_info:
            _ = cqrs_container.external_container

        assert "External container not attached" in str(exc_info.value)

    async def test_multiple_container_attachment(self) -> None:
        """
        Re-attaching container completely resets state and builds new mappings.

        Tests the behavior when attach_external_container() is called multiple times,
        which might occur in testing scenarios or during application reconfiguration.

        Verified Behaviors:
            1. First attachment builds mappings and enables resolution
            2. Second attachment clears all previous state:
               - Clears type-to-provider mapping
               - Clears provider lookup cache
               - Replaces container reference
            3. After re-attachment, only new container's types are resolvable
            4. Previous container's types are no longer accessible

        State Reset Process:
            - self._type_to_provider_path_map.clear()  # Removes old mappings
            - self._get_provider.cache_clear()         # Invalidates cached lookups
            - self._container = new_container          # Replaces reference
            - self._traverse_container(new_container)  # Builds new mappings

        This ensures clean separation between containers and prevents stale
        references or cross-contamination between different container instances.
        """
        cqrs_container = DependencyInjectorCQRSContainer()

        # First attachment: Container with UserRepository and UserService
        container1 = Container()
        cqrs_container.attach_external_container(container1)
        repo1 = await cqrs_container.resolve(UserRepository)
        assert isinstance(repo1, UserRepository)

        # Second attachment: SubdomainContainer with EmailNotificationService
        container2 = SubdomainContainer()
        cqrs_container.attach_external_container(container2)

        # New container's types are resolvable
        service = await cqrs_container.resolve(EmailNotificationService)
        assert isinstance(service, EmailNotificationService)

        # Previous container's types are no longer resolvable
        with pytest.raises(ValueError):
            await cqrs_container.resolve(UserRepository)
